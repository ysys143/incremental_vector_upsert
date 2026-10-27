"""Declared uniform-data baseline; advanced geometry/filter families remain separate."""
from vectorbench.evidence.contracts import Config


def cases():
    result = {}
    for operation, ratios in (("insert", (10,25,50,100)), ("delete", (10,25,50)),
                              ("churn", (50,)), ("upsert", (50,)), ("reinsert", (50,)),
                              ("same_vector", (50,)), ("metadata", (50,))):
        for ratio in ratios:
            for waves in (1,5):
                identity = f"{operation}-r{ratio}-w{waves}"
                result[identity] = {"id":identity,"operation":operation,"ratio":ratio,"waves":waves,
                                    "long":False,"hot":False,"policy":"settled"}
    for operation in ("churn","upsert"):
        for hot in (False,True):
            identity = f"{operation}-long-{'hot' if hot else 'uniform'}"
            result[identity] = {"id":identity,"operation":operation,"ratio":10,"waves":50,
                                "long":True,"hot":hot,"policy":"settled"}
    result["read_only-long"] = {"id":"read_only-long","operation":"read_only","ratio":0,"waves":50,
                                "long":True,"hot":False,"policy":"settled"}
    if len(result)!=29 or sum(v["waves"]+1 for v in result.values())!=351:
        raise AssertionError("baseline must contain 29 histories / 351 checkpoints per dataset/DB/seed")
    return result


def config():
    return Config()


def counts():
    units=2*2*len(config().seeds)
    return {"datasets":2,"databases":2,"seeds":list(config().seeds),"queries":config().query_rows,
            "histories":units*len(cases()),"incremental_points":units*sum(v["waves"]+1 for v in cases().values()),
            "fresh_points":"unique current ledgers: 3 final builds, 1 intermediate; counted after native input preparation",
            "scope":"UNIFORM_BASELINE_ONLY; geometry, continuous policy, recovery, online, retuning, filters remain required"}
