#!/usr/bin/env python3

from __future__ import print_function

import json
import time

from alibabacloud_tea_openapi.models import Config
from alibabacloud_credentials.client import Client as CredClient
from alibabacloud_pai_dlc20201203.client import Client as DLCClient
from alibabacloud_pai_dlc20201203.models import (
    ListJobsRequest,
    ListEcsSpecsRequest,
    CreateJobRequest,
    GetJobRequest,
)

from alibabacloud_aiworkspace20210204.client import Client as AIWorkspaceClient
from alibabacloud_aiworkspace20210204.models import (
    ListWorkspacesRequest,
    CreateDatasetRequest,
    ListDatasetsRequest,
    ListImagesRequest,
    ListCodeSourcesRequest
)


def create_nas_dataset(client, region, workspace_id, name,
                       nas_id, nas_path, mount_path):
    '''创建NAS的数据集。
    '''
    response = client.create_dataset(CreateDatasetRequest(
        workspace_id=workspace_id,
        name=name,
        data_type='COMMON',
        data_source_type='NAS',
        property='DIRECTORY',
        uri=f'nas://{nas_id}.{region}{nas_path}',
        accessibility='PRIVATE',
        source_type='USER',
        options=json.dumps({
            'mountPath': mount_path
        })
    ))
    return response.body.dataset_id


def create_oss_dataset(client, region, workspace_id, name,
                       oss_bucket, oss_endpoint, oss_path, mount_path):
    '''创建OSS数据集。
    '''
    response = client.create_dataset(CreateDatasetRequest(
        workspace_id=workspace_id,
        name=name,
        data_type='COMMON',
        data_source_type='OSS',
        property='DIRECTORY',
        uri=f'oss://{oss_bucket}.{oss_endpoint}{oss_path}',
        accessibility='PRIVATE',
        source_type='USER',
        options=json.dumps({
            'mountPath': mount_path
        })
    ))
    return response.body.dataset_id



def wait_for_job_to_terminate(client, job_id):
    while True:
        job = client.get_job(job_id, GetJobRequest()).body
        print('job({}) is {}'.format(job_id, job.status))
        if job.status in ('Succeeded', 'Failed', 'Stopped'):
            return job.status
        time.sleep(5)
    return None


def main():

    # 请确认您的主账号已授权DLC，且拥有足够的权限。
    region_id = 'cn-hangzhou'
    # 阿里云账号AccessKey拥有所有API的访问权限，建议您使用RAM用户进行API访问或日常运维。
    # 强烈建议不要把AccessKey ID和AccessKey Secret保存到工程代码里，否则可能导致AccessKey泄露，威胁您账号下所有资源的安全。
    # 本示例通过Credentials SDK默认从环境变量中读取AccessKey，来实现身份验证为例。
    cred = CredClient()

    # 1. create client;
    workspace_client = AIWorkspaceClient(
        config=Config(
            credential=cred,
            region_id=region_id,
            endpoint="aiworkspace.{}.aliyuncs.com".format(region_id),
        )
    )

    dlc_client = DLCClient(
         config=Config(
            credential=cred,
            region_id=region_id,
            endpoint='pai-dlc.{}.aliyuncs.com'.format(region_id),
         )
    )

    print('------- Workspaces -----------')
    # 获取工作空间列表。您也可以在参数workspace_name中填入您创建的工作空间名。
    workspaces = workspace_client.list_workspaces(ListWorkspacesRequest(
        page_number=1, page_size=1, workspace_name='',
        module_list='PAI'
    ))
    for workspace in workspaces.body.workspaces:
        print(workspace.workspace_name, workspace.workspace_id,
              workspace.status, workspace.creator)

    if len(workspaces.body.workspaces) == 0:
        raise RuntimeError('found no workspaces')

    workspace_id = workspaces.body.workspaces[0].workspace_id

    print('------- Images ------------')
    # 获取镜像列表。
    images = workspace_client.list_images(ListImagesRequest(
        labels=','.join(['system.supported.dlc=true',
                         'system.framework=Tensorflow 1.15',
                         'system.pythonVersion=3.6',
                         'system.chipType=CPU'])))
    for image in images.body.images:
        print(json.dumps(image.to_map(), indent=2))

    image_uri = images.body.images[0].image_uri

    print('------- Datasets ----------')
    # 获取数据集。
    datasets = workspace_client.list_datasets(ListDatasetsRequest(
        workspace_id=workspace_id,
        name='example-nas-data', properties='DIRECTORY'))
    for dataset in datasets.body.datasets:
        print(dataset.name, dataset.dataset_id, dataset.uri, dataset.options)

    if len(datasets.body.datasets) == 0:
        # 当前数据集不存在时，创建数据集。
        dataset_id = create_nas_dataset(
            client=workspace_client,
            region=region_id,
            workspace_id=workspace_id,
            name='example-nas-data',
            # Nas文件系统ID。
            # 通用型NAS：31a8e4****。
            # 极速型NAS：必须以extreme-开头，例如extreme-0015****。
            # CPFS：必须以cpfs-开头，例如cpfs-125487****。
            nas_id='***',
            nas_path='/',
            mount_path='/mnt/data/nas')
        print('create dataset with id: {}'.format(dataset_id))
    else:
        dataset_id = datasets.body.datasets[0].dataset_id

    print('------- Code Sources ----------')
    # 获取代码集列表。
    code_sources = workspace_client.list_code_sources(ListCodeSourcesRequest(
        workspace_id=workspace_id))
    for code_source in code_sources.body.code_sources:
        print(code_source.display_name, code_source.code_source_id, code_source.code_repo)

    print('-------- ECS SPECS ----------')
    # 获取DLC的节点规格列表。
    ecs_specs = dlc_client.list_ecs_specs(ListEcsSpecsRequest(page_size=100, sort_by='Memory', order='asc'))
    for spec in ecs_specs.body.ecs_specs:
        print(spec.instance_type, spec.cpu, spec.memory, spec.memory, spec.gpu_type)

    print('-------- Create Job ----------')
    # 创建DLC作业。
    create_job_resp = dlc_client.create_job(CreateJobRequest().from_map({
        'WorkspaceId': workspace_id,
        'DisplayName': 'sample-dlc-job',
        'JobType': 'TFJob',
        'JobSpecs': [
            {
                "Type": "Worker",
                "Image": image_uri,
                "PodCount": 1,
                "EcsSpec": ecs_specs.body.ecs_specs[0].instance_type,
            },
        ],
        "UserCommand": "echo 'Hello World' && ls -R /mnt/data/ && sleep 30 && echo 'DONE'",
        'DataSources': [
            {
                "DataSourceId": dataset_id,
            },
        ],
    }))
    job_id = create_job_resp.body.job_id

    wait_for_job_to_terminate(dlc_client, job_id)

    print('-------- List Jobs ----------')
    # 获取DLC的作业列表。
    jobs = dlc_client.list_jobs(ListJobsRequest(
        workspace_id=workspace_id,
        page_number=1,
        page_size=10,
    ))
    for job in jobs.body.jobs:
        print(job.display_name, job.job_id, job.workspace_name,
              job.status, job.job_type)
    pass


if __name__ == '__main__':
    main()