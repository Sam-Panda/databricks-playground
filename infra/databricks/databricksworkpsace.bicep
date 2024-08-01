param workspaceName string
param location string
param pricingTier string
param managedResourceGroupId string
param vnetid string
param publicSubnetName string
param privateSubnetName string
param disablePublicIp bool
param publicNetworkAccess string
param requiredNsgRules string


resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: workspaceName
  location: location
  sku: {
    name: pricingTier
  }
  properties: {
    managedResourceGroupId: managedResourceGroupId
    parameters: {
      customVirtualNetworkId: {
        value: vnetid
      }
      customPublicSubnetName: {
        value: publicSubnetName
      }
      customPrivateSubnetName: {
        value: privateSubnetName
      }
      enableNoPublicIp: {
        value: disablePublicIp
      }
    }
    publicNetworkAccess: publicNetworkAccess
    requiredNsgRules: requiredNsgRules
  }
}

output workspaceId string = workspace.id

