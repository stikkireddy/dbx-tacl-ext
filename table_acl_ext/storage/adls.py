from typing import List

from adlfs import AzureBlobFileSystem

from table_acl_ext.storage import CredentialsManager, Widget, DatabricksSecret, TextWidget, SerializableStorageWrapper, \
    get_scope


class ADLSSPCredentialsManager(CredentialsManager):
    ACCOUNT_NAME = "account_name"
    CLIENT_ID = "client_id"
    CLIENT_SECRET = "client_secret"
    TENANT_ID = "tenant_id"
    GROUPS = "groups"

    def get_databricks_secrets(self) -> List[DatabricksSecret]:
        widgets = self.widgets()
        widget_values = {widget.get_name(): widget.get() for widget in widgets}
        groups = widget_values[self.GROUPS]
        secret_value = SerializableStorageWrapper(AzureBlobFileSystem, options={
            self.ACCOUNT_NAME: widget_values[self.ACCOUNT_NAME],
            self.CLIENT_ID: widget_values[self.CLIENT_ID],
            self.CLIENT_SECRET: widget_values[self.CLIENT_SECRET],
            self.TENANT_ID: widget_values[self.TENANT_ID],
        }).serialized_fs
        return [
            DatabricksSecret(
                scope=get_scope(group),
                key=widget_values[self.ACCOUNT_NAME],
                secret_string=secret_value,
                group=group
            ) for group in groups.split(",")]

    def widgets(self) -> List[Widget]:
        return [
            TextWidget(self.ACCOUNT_NAME, "", "Storage Account Name"),
            TextWidget(self.CLIENT_ID, "", "App Client ID"),
            TextWidget(self.CLIENT_SECRET, "", "App Client Secret"),
            TextWidget(self.TENANT_ID, "", "Azure AAD Tenant ID"),
            TextWidget(self.GROUPS, "", "Groups"),
        ]


class ADLSAccessKeyCredentialsManager(CredentialsManager):
    ACCOUNT_NAME = "account_name"
    ACCOUNT_KEY = "account_key"
    GROUPS = "groups"

    def get_databricks_secrets(self) -> List[DatabricksSecret]:
        widgets = self.widgets()
        widget_values = {widget.get_name(): widget.get() for widget in widgets}
        groups = widget_values[self.GROUPS]
        secret_value = SerializableStorageWrapper(AzureBlobFileSystem, options={
            self.ACCOUNT_NAME: widget_values[self.ACCOUNT_NAME],
            self.ACCOUNT_KEY: widget_values[self.ACCOUNT_KEY],
        }).serialized_fs
        return [
            DatabricksSecret(
                scope=get_scope(group),
                key=widget_values[self.ACCOUNT_NAME],
                secret_string=secret_value,
                group=group
            ) for group in groups.split(",")]

    def widgets(self) -> List[Widget]:
        return [
            TextWidget(self.ACCOUNT_NAME, "", "Storage Account Name"),
            TextWidget(self.ACCOUNT_KEY, "", "Storage Account Key"),
            TextWidget(self.GROUPS, "", "Groups"),
        ]
