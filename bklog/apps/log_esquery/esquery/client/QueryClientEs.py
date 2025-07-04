# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
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
from typing import Any, Dict

from django.conf import settings
from django.utils.translation import gettext as _
from elasticsearch import Elasticsearch as Elasticsearch
from elasticsearch5 import Elasticsearch as Elasticsearch5

from apps.api import TransferApi
from apps.log_esquery.constants import DEFAULT_SCHEMA
from apps.log_esquery.esquery.client.QueryClientTemplate import QueryClientTemplate
from apps.log_esquery.exceptions import (
    BaseSearchFieldsException,
    BaseSearchIndexSettingsException,
    EsClientAliasException,
    EsClientCatIndicesException,
    EsClientMetaInfoException,
    EsClientScrollException,
    EsClientSearchException,
    EsException,
)
from apps.log_esquery.type_constants import type_mapping_dict
from apps.log_esquery.utils.es_client import es_socket_ping, get_es_client
from apps.utils.cache import cache_five_minute


class QueryClientEs(QueryClientTemplate):  # pylint: disable=invalid-name
    def __init__(self, storage_cluster_id: int):
        super(QueryClientEs, self).__init__()

        self.storage_cluster_id = storage_cluster_id
        self._client: Elasticsearch

    def query(self, index: str, body: Dict[str, Any], scroll=None, track_total_hits=False):
        self._build_connection(check_ping=False)

        # 如果版本不是5.0且track_total_hits为True时
        if track_total_hits and not isinstance(self._client, Elasticsearch5):
            body.update({"track_total_hits": True})

        try:
            params = {"request_timeout": settings.ES_QUERY_TIMEOUT}
            return self._client.search(index=index, body=body, scroll=scroll, params=params)
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise EsClientSearchException(EsClientSearchException.MESSAGE.format(error=e))

    def mapping(self, index: str, add_settings_details: bool = False) -> Dict:
        self._build_connection(check_ping=False)
        try:
            mapping_dict: type_mapping_dict = self._client.indices.get_mapping(index=index)
            if add_settings_details:
                settings_dict: Dict = self.get_settings(index=index)
                return self.add_analyzer_details(_mappings=mapping_dict, _settings=settings_dict)
            return mapping_dict
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise BaseSearchFieldsException(BaseSearchFieldsException.MESSAGE.format(error=e))

    def get_settings(self, index: str) -> Dict:
        try:
            return self._client.indices.get_settings(index=index)
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise BaseSearchIndexSettingsException(BaseSearchIndexSettingsException.MESSAGE.format(error=e))

    def scroll(self, index: str, scroll_id: str, scroll: str) -> Dict:
        self._build_connection(check_ping=False)
        try:
            return self._client.scroll(scroll_id=scroll_id, scroll=scroll)
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise EsClientScrollException(EsClientScrollException.MESSAGE.format(error=e))

    def cluster_stats(self, index=None):
        self._build_connection()
        try:
            return self._client.cluster.stats()
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise EsException

    def cat_indices(self, index=None, bytes="mb", format="json", params=None):
        if params is None:
            params = {"request_timeout": 10}
        self._build_connection()
        try:
            return self._client.cat.indices(index=index, bytes=bytes, format=format, params=params)
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise EsClientCatIndicesException(EsClientCatIndicesException.MESSAGE.format(error=e))

    def cluster_nodes_stats(self, index=None):
        self._build_connection()
        return self._client.nodes.stats()

    def es_route(self, url: str, index=None):
        self._build_connection()
        if not url.startswith("/"):
            url = "/" + url
        try:
            return self._client.transport.perform_request("GET", url)
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise

    def _build_connection(self, check_ping: bool = True):
        if not self._active:
            self._get_connection(check_ping=check_ping)
            if check_ping and not self._active:
                raise EsClientSearchException(EsClientSearchException.MESSAGE.format(error=_("EsClient链接失败")))
            else:
                pass
        else:
            pass

    def _get_connection(self, check_ping: bool = True):
        self.host, self.port, self.username, self.password, self.version, self.schema = self._connect_info(
            storage_cluster_id=self.storage_cluster_id
        )
        self._active: bool = False

        # es socket ping
        es_socket_ping(host=self.host, port=self.port)

        self._client: Elasticsearch = get_es_client(
            version=self.version,
            hosts=[self.host],
            username=self.username,
            password=self.password,
            scheme=self.schema,
            port=self.port,
            sniffer_timeout=600,
            verify_certs=False,
        )
        if not check_ping or self._client.ping():
            self._active = True

    @staticmethod
    @cache_five_minute("_connect_info_{storage_cluster_id}", need_md5=True)
    def _connect_info(storage_cluster_id: int) -> tuple:
        transfer_api_response: list = TransferApi.get_cluster_info({"cluster_id": storage_cluster_id})
        if len(transfer_api_response) == 1:
            data_list: list = transfer_api_response
            cluster_config_dict: dict = data_list[0].get("cluster_config")
            domain_name: str = cluster_config_dict.get("domain_name", "")
            port: int = cluster_config_dict.get("port", -1)
            version: str = cluster_config_dict.get("version", "")
            auth_info_dict: dict = data_list[0].get("auth_info")
            username: str = auth_info_dict.get("username", "")
            password: str = auth_info_dict.get("password", "")
            # 添加协议字段 由于是后添加的 所以放置在这个地方
            schema: str = cluster_config_dict.get("schema") or DEFAULT_SCHEMA

            _es_password = password
            _es_host = domain_name
            _es_port = port
            _es_user = username
            _es_version = version
            _es_schema = schema

            return _es_host, _es_port, _es_user, _es_password, _es_version, _es_schema

        else:
            raise EsClientMetaInfoException(EsClientMetaInfoException.MESSAGE.format(message="meta_api_response error"))

    def indices(self, bk_biz_id=None, result_table_id=None, with_storage=False):
        """
        查询索引列表
        :param bk_biz_id:
        :param result_table_id:
        :param with_storage:
        :return:
        """
        self._build_connection()
        try:
            index_results = self._client.indices.get(result_table_id if result_table_id else "*")
        except Exception as e:  # pylint: disable=broad-except
            self.catch_timeout_raise(e)
            raise EsClientAliasException(EsClientAliasException.MESSAGE.format(error=e))
        index_list = [{"result_table_id": item, "result_table_name_alias": item} for item in index_results.keys()]

        # 补充索引集群信息
        if with_storage and index_list:
            cluster_config = self.get_cluster_info()
            for _index in index_list:
                _index.update(
                    {
                        "storage_cluster_id": cluster_config.get("storage_cluster_id"),
                        "storage_cluster_name": cluster_config.get("storage_cluster_name"),
                    }
                )
        return index_list

    def get_cluster_info(self, result_table_id=None):
        result = TransferApi.get_cluster_info({"cluster_id": self.storage_cluster_id})
        if not result:
            raise EsClientMetaInfoException(EsClientMetaInfoException.MESSAGE.format(message="meta_api_response error"))
        cluster_info = result[0]
        cluster_config: dict = cluster_info.get("cluster_config", {})
        return {
            "cluster_type": cluster_info["cluster_type"],
            "storage_cluster_name": cluster_config["cluster_name"],
            "storage_cluster_id": cluster_config["cluster_id"],
            "version": cluster_config["version"],
            "registered_system": cluster_config["registered_system"],
        }
