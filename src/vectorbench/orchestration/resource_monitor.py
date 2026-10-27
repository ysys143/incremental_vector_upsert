"""Owned Docker CLI sampling; the child stays in its worker process group."""
import json
import subprocess
import threading
import time
import re
import os
import shutil
from vectorbench.evidence.contracts import save


def parse_sample(line):
    # Docker renders terminal control sequences even with a custom JSON template here.
    clean=re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]","",line).strip()
    return json.loads(clean) if clean else None


class Monitor:
    def __init__(self, token, out):
        self.token=token
        self.extended=os.environ.get('REAL_STUDY_CLUSTERED_RESOURCES')=='1'
        self.out,self.samples,self.error=out,0,None
        self.log=(out/"resource-samples.jsonl").open("w")
        self.err=(out/"resource-sampler-stderr.log").open("w")
        self.started=time.time()
        self.process=subprocess.Popen(["docker","stats","--no-trunc","--format","{{json .}}",token],
            stdout=subprocess.PIPE,stderr=self.err,text=True,bufsize=1)
        self.thread=threading.Thread(target=self.collect,daemon=True)
        self.thread.start()

    def collect(self):
        try:
            for line in self.process.stdout:
                sample=parse_sample(line)
                if sample is None: continue
                record={"time":time.time(),"docker":sample}
                if self.extended and self.samples % 5 == 0:
                    rss=subprocess.run(['docker','exec',self.token,'sh','-c',
                        "awk '/^VmRSS:/ {s+=$2} END {print s+0}' /proc/[0-9]*/status"],
                        capture_output=True,text=True,timeout=10)
                    disk=subprocess.run(['docker','exec',self.token,'sh','-c',
                        'du -sk /var/lib/postgresql/data /qdrant/storage 2>/dev/null'],
                        capture_output=True,text=True,timeout=10)
                    if rss.returncode==0 and rss.stdout.strip().isdigit():
                        record['summed_process_rss_bytes']=int(rss.stdout.strip())*1024
                    sizes=[int(x.split()[0])*1024 for x in disk.stdout.splitlines() if x.split() and x.split()[0].isdigit()]
                    if sizes:record['database_disk_bytes']=sum(sizes)
                    record['host_free_bytes']=shutil.disk_usage(self.out).free
                    record['rss_scope']='sampled sum of Linux process VmRSS; shared pages may be counted repeatedly'
                self.log.write(json.dumps(record)+"\n")
                self.log.flush(); self.samples+=1
        except BaseException as exc:
            self.error=repr(exc)

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
        forced=False
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            forced=True; self.process.kill(); self.process.wait(timeout=5)
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("owned resource sampler did not stop")
        self.process.stdout.close(); self.log.close(); self.err.close()
        acceptable=self.process.returncode in (0,-15) or (forced and self.process.returncode==-9)
        result={"status":"PASS" if self.samples and self.error is None and acceptable else "UNVERIFIED",
                "started":self.started,"finished":time.time(),"samples":self.samples,"error":self.error,
                "sampler_killed_after_grace":forced,
                "scope":"approximately 1s Docker samples, not an exact continuous peak"}
        save(self.out/"resource-monitor.json",result)
        return [] if result["status"]=="PASS" else ["resource monitoring incomplete: "+str(result)]
