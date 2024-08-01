param accessConnectorName string
param location string
param tags object


resource symbolicname 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: 'string'
  location: 'string'
  tags: {
    tagName1: 'tagValue1'
    tagName2: 'tagValue2'
  }
  identity: {
    type: 'string'
    userAssignedIdentities: {
      {customized property}: {}
    }
  }
  properties: {}
}
