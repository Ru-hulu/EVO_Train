from alibabacloud_pai_dlc20201203.client import Client
from alibabacloud_credentials.client import Client as CredClient
from alibabacloud_tea_openapi.models import Config
from alibabacloud_pai_dlc20201203.models import (
    CreateJobRequest,
    JobSpec,
    ResourceConfig, GetJobRequest
)

# 初始化一个Client，用来访问DLC的API。
region = 'cn-hangzhou'
# 阿里云账号AccessKey拥有所有API的访问权限，建议您使用RAM用户进行API访问或日常运维。
# 强烈建议不要把AccessKey ID和AccessKey Secret保存到工程代码里，否则可能导致AccessKey泄露，威胁您账号下所有资源的安全。
# 本示例通过Credentials SDK默认从环境变量中读取AccessKey，来实现身份验证为例。
cred = CredClient()
client = Client(
    config=Config(
        credential=cred,
        region_id=region,
        endpoint=f'pai-dlc.{region}.aliyuncs.com',
    )
)

# 声明任务的资源配置，关于镜像选择可以参考文档中公共镜像列表，也可以传入自己的镜像地址。
spec = JobSpec(
    type='Worker',
    image=f'registry-vpc.cn-hangzhou.aliyuncs.com/pai-dlc/ppu-training:2.0.0-pytorch2.9.0-ppu-py312-cu129-ubuntu24.04',
    pod_count=1,
    resource_config=ResourceConfig(cpu='1', memory='2Gi')
)

# 声明任务的执行内容。
req = CreateJobRequest(
        resource_id='quota8xa9l64xn9c', # 替换成您自己的资源配额ID
        workspace_id='604723', #<替换成您自己的WorkspaceID>
        display_name='ru-dlc-job',
        job_type='TFJob',
        job_specs=[spec],
        user_command='echo "Hello World"',
)

# 提交任务。
response = client.create_job(req)
# 获取任务ID。
job_id = response.body.job_id

# 查询任务状态。
job = client.get_job(job_id, GetJobRequest()).body
print('job status:', job.status)

# 查看任务执行的命令。
job.user_command