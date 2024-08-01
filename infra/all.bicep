param location string = resourceGroup().location
param resourceTags object = {
  Application: 'databroicks-test'
  CostCenter: 'Az-Sales'  
  Environment: 'Development'
  Owner: 'sapa@microsoft.com'
}


var vnetName = 'my-contoso-vnet'
var storageAcountName = '3207${uniqueString(resourceGroup().name)}'


//deploying the vnet

module network 'network/vnet.bicep' = {
  name: vnetName
  params: {
    resourceTags: resourceTags
    vnetName: vnetName
    location: location
  }
}

//deploying the storage account

module storage 'storage/storage.bicep' = {
  name: storageAcountName
  params: {
    resourceTags: resourceTags
    location: location
    name: storageAcountName
    vnetId: network.outputs.vnetId
    privateLinkSubnetId: network.outputs.privateLinkSubnetId
    
  }
}

// module functionApp 'Functions/function.bicep' = {
//   name: functionAppName
//   dependsOn: [
//     storage    
//   ]
//   params: {
//     appName: functionAppName
//     planName: '${functionAppName}plan'
//     location: location    
//     subnetId: network.outputs.paasSubnetId
//     storageAccountName: storage.outputs.name
//     storageAccountKey: storage.outputs.key
//     contentShareName: functionContentShareName
//     appInsightName: '${functionAppName}-ai'
//     resourceTags: resourceTags
//   }
// }
