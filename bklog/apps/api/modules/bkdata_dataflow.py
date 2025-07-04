"""
Tencent is pleased to support the open source community by making BK-LOG 蓝鲸日志平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-LOG 蓝鲸日志平台 is licensed under the MIT License.
License for BK-LOG 蓝鲸日志平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
We undertake not to change the open source license (MIT license) applicable to the current version of
the project delivered to anyone in the future.
"""

from django.utils.translation import gettext_lazy as _  # noqa

from apps.api.base import DataAPI  # noqa
from apps.api.modules.utils import add_esb_info_before_request_for_bkdata_user, biz_to_tenant_getter  # noqa
from config.domains import DATAFLOW_APIGATEWAY_ROOT  # noqa


class _BkDataDataFlowApi:
    MODULE = _("数据平台dataflow模块")

    def __init__(self):
        self.export_flow = DataAPI(
            method="GET",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/export/",
            url_keys=["flow_id"],
            module=self.MODULE,
            description="导出flow",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
        )
        self.create_flow = DataAPI(
            method="POST",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/create/",
            module=self.MODULE,
            description="创建flow",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            default_timeout=300,
            bk_tenant_id=biz_to_tenant_getter(),
        )
        self.start_flow = DataAPI(
            method="POST",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/start/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="启动flow",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
        )
        self.stop_flow = DataAPI(
            method="POST",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/stop/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="停止flow",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
        )
        self.restart_flow = DataAPI(
            method="POST",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/restart/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="重启flow",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
        )
        self.get_flow_graph = DataAPI(
            method="GET",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/graph/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="获取DataFlow图信息",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            bk_tenant_id=biz_to_tenant_getter(),
        )
        self.add_flow_nodes = DataAPI(
            method="POST",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/nodes/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="新增节点",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            bk_tenant_id=biz_to_tenant_getter(key=lambda p: p["config"]["bk_biz_id"]),
        )

        self.get_latest_deploy_data = DataAPI(
            method="GET",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/latest_deploy_data/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="获取flow最近部署信息",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            bk_tenant_id=biz_to_tenant_getter(),
        )
        self.get_dataflow = DataAPI(
            method="GET",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/",
            module=self.MODULE,
            url_keys=["flow_id"],
            description="获取flow信息",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            bk_tenant_id=biz_to_tenant_getter(),
        )
        self.patch_flow_nodes = DataAPI(
            method="PATCH",
            url=DATAFLOW_APIGATEWAY_ROOT + "flow/flows/{flow_id}/nodes/{node_id}/",
            module=self.MODULE,
            url_keys=["flow_id", "node_id"],
            description="部分更新节点",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            bk_tenant_id=biz_to_tenant_getter(),
        )
        self.set_dataflow_resource = DataAPI(
            method="POST",
            url=DATAFLOW_APIGATEWAY_ROOT + "tool/job/set_resource/",
            module=self.MODULE,
            description="设置dataflow资源",
            before_request=add_esb_info_before_request_for_bkdata_user,
            after_request=None,
            bk_tenant_id=biz_to_tenant_getter(),
        )


BkDataDataFlowApi = _BkDataDataFlowApi()
