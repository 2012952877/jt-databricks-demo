# 文件路径: tests/test_iac_compliance.py
import yaml
import pytest

def test_cluster_node_type_compliance():
    """强制校验：J&T UAT 环境仅允许使用 Standard_D4s_v5 机型"""
    with open("resources/jt_jobs.yml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    jobs = config.get("resources", {}).get("jobs", {})
    for name, job in jobs.items():
        for cluster in job.get("job_clusters", []):
            node_type = cluster.get("new_cluster", {}).get("node_type_id")
            # 💡 拦截逻辑：非 D4s_v5 机型直接阻断发布
            assert node_type == "Standard_D4s_v5", f"违规！作业 [{name}] 使用了禁用的机型 {node_type}"