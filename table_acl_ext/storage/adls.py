from typing import List

from adlfs import AzureBlobFileSystem

from table_acl_ext.storage import CredentialsManager, Widget, DatabricksSecret, TextWidget, SerializableStorageWrapper


def get_scope(group_name):
    return f"__storage__{group_name}"


class ADLSSPCredentialsManager(CredentialsManager):
    ACCOUNT_NAME = "account_name"
    CLIENT_ID = "client_id"
    CLIENT_SECRET = "client_secret"
    TENANT_ID = "tenant_id"
    GROUPS = "groups"

    def get_databricks_secrets(self) -> List[DatabricksSecret]:
        widgets = self.widgets()
        widget_values = {widget.name(): widget.get() for widget in widgets}
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
            TextWidget(self.GROUPS, "", "Azure AAD Tenant ID"),
        ]
