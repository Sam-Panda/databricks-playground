@description('Tags for all resources.')
param resourceTags object = {
  Application: 'databroicks-test'
  CostCenter: 'Az-Sales'  
  Environment: 'Development'
  Owner: 'sapa@microsoft.com'
}


@description('Specifies whether to deploy Azure Databricks workspace with secure cluster connectivity (SCC) enabled or not (No Public IP).')
param disablePublicIp bool = true

@description('Location for all resources.')
param location string = resourceGroup().location

@description('The name of the network security group to create.')
param nsgName string = 'databricks-nsg'

@description('The pricing tier of workspace.')
@allowed([
  'trial'
  'standard'
  'premium'
])
param pricingTier string = 'premium'

@description('CIDR range for the private subnet.')
param privateSubnetCidr string = '10.179.0.0/18'

@description('The name of the private subnet to create.')
param privateSubnetName string = 'private-subnet'

@description('Indicates whether public network access is allowed to the workspace with private endpoint - possible values are Enabled or Disabled.')
@allowed([
  'Enabled'
  'Disabled'
])
param publicNetworkAccess string = 'Enabled'

@description('CIDR range for the public subnet.')
param publicSubnetCidr string = '10.179.64.0/18'

@description('CIDR range for the private endpoint subnet..')
param privateEndpointSubnetCidr string = '10.179.128.0/24'

@description('The name of the public subnet to create.')
param publicSubnetName string = 'public-subnet'

@description('Indicates whether to retain or remove the AzureDatabricks outbound NSG rule - possible values are AllRules or NoAzureDatabricksRules.')
@allowed([
  'AllRules'
  'NoAzureDatabricksRules'
])
param requiredNsgRules string = 'NoAzureDatabricksRules'

@description('CIDR range for the vnet.')
param vnetCidr string = '10.179.0.0/16'

@description('The name of the virtual network to create.')
param vnetName string = 'my-contoso-vnet'

@description('The name of the subnet to create the private endpoint in.')
param PrivateEndpointSubnetName string = 'private-endpoint-subnet'

@description('The name of the Azure Databricks workspace to create.')
param workspaceName string = 'default'

// The below 2 subnet is for external ADLS gen2 that we are creating. 
param privateLinkSubnetName string = 'privatelink-subnet'
param privateLinkSubnetAddressSpace string = '10.179.129.0/27'

param paasSubnetName string = 'paas-subnet'
param paasSubnetAddressSpace string = '10.179.130.0/28'

param databricksnsgName string = 'databricks-nsg'

param storageAccountName string = '3207${uniqueString(resourceGroup().name)}'

var managedResourceGroupName = 'databricks-rg-${workspaceName}-${uniqueString(workspaceName, resourceGroup().id)}'
var trimmedMRGName = substring(managedResourceGroupName, 0, min(length(managedResourceGroupName), 90))
var managedResourceGroupId = '${subscription().id}/resourceGroups/${trimmedMRGName}'
var privateEndpointName = '${workspaceName}-pvtEndpoint'
var privateDnsZoneName = 'privatelink.azuredatabricks.net'
var pvtEndpointDnsGroupName = '${privateEndpointName}/mydnsgroupname'

module network1 './network/vnet.bicep' = {
  name: vnetName
  params: {
    location: location
    vnetName: vnetName
    resourceTags: resourceTags
    vnetAddressSpace: vnetCidr
    publicSubnetName: publicSubnetName
    publicSubnetCidr: publicSubnetCidr
    privateSubnetName: privateSubnetName
    privateSubnetCidr: privateSubnetCidr
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    privateEndpointSubnetCidr: privateEndpointSubnetCidr
    privateLinkSubnetName: privateLinkSubnetName
    privateLinkSubnetCidr: privateLinkSubnetAddressSpace
    paasSubnetName: paasSubnetName
    paasSubnetCidr: paasSubnetAddressSpace
    databricksnsgName: databricksnsgName
  }
}

module databricksWorkspace 'databricks/databricksworkpsace.bicep' = {
  name: workspaceName
  params: {
    workspaceName: workspaceName
    location: location
    pricingTier: pricingTier
    managedResourceGroupId: managedResourceGroupId
    vnetid: network1.outputs.vnetId
    publicSubnetName: publicSubnetName
    privateSubnetName: privateSubnetName
    disablePublicIp: disablePublicIp
    publicNetworkAccess: publicNetworkAccess
    requiredNsgRules: requiredNsgRules
  }
}

module databricksprivateEndpoint './network/databricksPE.bicep' = {
  name: privateEndpointName
  params: {
    privateEndpointName: privateEndpointName
    location: location
    VnetId: network1.outputs.vnetId
    vnetName: vnetName
    PrivateEndpointSubnetName: PrivateEndpointSubnetName
    workspaceId: databricksWorkspace.outputs.workspaceId
    privateDnsZoneName: privateDnsZoneName
    pvtEndpointDnsGroupName: pvtEndpointDnsGroupName   

  }
  dependsOn: [
    databricksWorkspace
    network1
  ]
}
 

module extternalstorageAccount './storage/storage.bicep' = {
  name: 'storage'
  params: {
    location: location
    name: storageAccountName
    resourceTags: resourceTags
    vnetId: network1.outputs.vnetId
    privateLinkSubnetId: network1.outputs.privateLinkSubnetId
  }
}
