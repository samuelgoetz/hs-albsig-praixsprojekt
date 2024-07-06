provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "rg-albsig" {
  name     = "rg-albsig"
  location = "East US"
}

resource "azurerm_storage_account" "sago-albsig" {
  name                     = "sago-albsig"
  resource_group_name      = azurerm_resource_group.rg-albsig.name
  location                 = azurerm_resource_group.rg-albsig.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_mssql_server" "hsalbsigserver" {
  name                         = "hsalbsigserver"
  resource_group_name          = azurerm_resource_group.rg-albsig.name
  location                     = azurerm_resource_group.rg-albsig.location
  version                      = "12.0"
  administrator_login          = "massiverfrosch"
  administrator_login_password = "4-v3ry-53cr37-p455w0rd"
}

resource "azurerm_mssql_database" "hsalbsigdev" {
  name                = "hsalbsigdev"
  server_id           = azurerm_mssql_server.hsalbsigserver.id
  sku_name            = "Basic"
  max_size_gb         = 32
}


