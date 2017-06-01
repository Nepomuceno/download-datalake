using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;

namespace ImportFiles
{
    class Program
    {
        private static DataLakeStoreAccountManagementClient _adlsClient;
        private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;

        private static string _adlsAccountName;
        private static string _resourceGroupName;
        private static string _location;
        private static string _subId;

        private static void Main(string[] args)
        {
            _adlsAccountName = "DataLakeName"; // TODO: Replace this value with the name of your existing Data Lake Store account.
            _resourceGroupName = "ResourceGroupName"; // TODO: Replace this value with the name of the resource group containing your Data Lake Store account.
            _location = "East US 2";
            _subId = "SubscriptionId";

            string localFolderPath = @"C:\reports\"; // TODO: Make sure this exists and can be overwritten.
            string remoteFolderPath = "/poc/facdata3";
            string remoteFilePath = Path.Combine(remoteFolderPath, "file.txt");

            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var tenant_id = "TenantId"; // Replace this string with the user's Azure Active Directory tenant ID
            var nativeClientApp_clientId = "1950a258-227b-4e31-a9cf-717495945fc2"; // This is ok it is general for all the azure Dont change
            var activeDirectoryClientSettings = ActiveDirectoryClientSettings.UsePromptOnly(nativeClientApp_clientId, new Uri("urn:ietf:wg:oauth:2.0:oob")); //Dont change this is one for all azure.
            var creds = UserTokenProvider.LoginWithPromptAsync(tenant_id, activeDirectoryClientSettings).Result;

            // Create client objects and set the subscription ID
            _adlsClient = new DataLakeStoreAccountManagementClient(creds) { SubscriptionId = _subId };
            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);

            var remoteFiles = ListItems(remoteFolderPath);
            foreach (var file in remoteFiles)
            {
                string localFilePath = Path.Combine(localFolderPath, file.PathSuffix); // TODO: Make sure this exists and can be overwritten.
                DownloadFile($"{remoteFolderPath}/{file.PathSuffix}", localFilePath).Wait();
            }
            Console.ReadLine();



            /*
             * {
       "data": {
            "baseType":"OpenSchemaData",
            "baseData":{
               "ver":"2",
               "blobSasUri":"<Blob URI with Shared Access Key>",
               "sourceName":"<Schema ID>",
               "sourceVersion":"1.0"
             }
       },
       "ver":1,
       "name":"Microsoft.ApplicationInsights.OpenSchema",
       "time":"<DateTime>",
       "iKey":"<instrumentation key>"
    }
             */

            //SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            //var domain = "<AAD-directory-domain>";
            //var webApp_clientId = "<AAD-application-clientid>";
            //var clientSecret = "<AAD-application-client-secret>";
            //var clientCredential = new ClientCredential(webApp_clientId, clientSecret);
            //var creds = await ApplicationTokenProvider.LoginSilentAsync(domain, clientCredential);
        }

        // List all ADLS accounts within the subscription
        public static async Task<List<DataLakeStoreAccount>> ListAdlStoreAccounts()
        {
            var response = await _adlsClient.Account.ListAsync();
            var accounts = new List<DataLakeStoreAccount>(response);

            while (response.NextPageLink != null)
            {
                response = _adlsClient.Account.ListNext(response.NextPageLink);
                accounts.AddRange(response);
            }

            return accounts;
        }

        // List files and directories
        public static List<FileStatusProperties> ListItems(string directoryPath)
        {
            return _adlsFileSystemClient.FileSystem.ListFileStatus(_adlsAccountName, directoryPath).FileStatuses.FileStatus.ToList();
        }

        // Download file
        public static async Task DownloadFile(string srcPath, string destPath)
        {
            using (var stream = await _adlsFileSystemClient.FileSystem.OpenAsync(_adlsAccountName, srcPath))
            using (var fileStream = new StreamWriter(destPath,false,Encoding.UTF8))
            using (var streamReader = new StreamReader(stream))
            {
                while (!streamReader.EndOfStream)
                {
                    var line = await streamReader.ReadLineAsync();
                    var result = line.Replace("'", "");
                    await fileStream.WriteLineAsync(result);
                }
            }
        }
    }
}
