## Validating certificates

### OCSP and CRL
At the moment of writing (autumn 2024) Snowflake .NET driver still uses a method to verify the validity
of certificates upon connection, which is different from all the other Snowflake drivers.
* .NET driver - uses Certificate Revocation List  (CRL) ([wikipage](https://en.wikipedia.org/wiki/Certificate_revocation_list))
* all other Snowflake drivers (JDBC, ODBC, Python, etc.) - use Online Certificate Status Protocol (OCSP) ([Snowflake documentation for OCSP](https://docs.snowflake.com/en/user-guide/ocsp))

Therefore all network reachability requirements we documents for OCSP endpoints, also apply here, only difference that instead of the OCSP endpoints exposed by the certificate authorities,
their CRL endpoints must be reachable.

Both methods operate on port 80, unencrypted HTTP. Click [here](https://community.snowflake.com/s/article/Why-do-OCSP-use-Port-80-and-not-Port-443) if you wish to read more about the reasons.
The certificate authorities operating their CRL/OCSP services over unencrypted HTTP does not introduce any security risk in itself, because no customer data is actually transmitted unencrypted.
Only the data necessary for verifying certificates, which is publicly available information.

Without the CRL endpoints being properly reachable for the .NET driver, it won't be able to verify certificate validity, and attempting to connect to Snowflake-related endpoints might suffer unnecessary delays, and eventually time out or break with an error:
```c#
error:System.Security.Authentication.AuthenticationException:
The remote certificate is invalid because of errors in the certificate chain: RevocationStatusUnknown, OfflineRevocation
```

:warning: Above error message can be also a result of a proxy or other network appliance performing SSL inspection on the connection made to Snowflake. If such an appliance injects its own CA certificate into the chain of trust, the driver won’t be able to properly verify it using the public CRL endpoints.

### What are the relevant CRL endpoints
These endpoints are not managed or operated by Snowflake - they are part of the public key infrastructure and are provided and managed by the various certificate authorities.
They can be extracted from the particular certificate which is part of the chain upon connecting to the endpoint (Snowflake, cloud storage).

Many methods are available to read and parse certificates and extract various information from them using the different programming languages.
We'll demonstrate a simple shell script method using the toolkit generally available on Linux and Mac hosts.
For Windows, the exact same tools are also available, but possibly you'll need to download them first if you decide to use this same method for verification - mostly `openssl` and `curl` or `wget`.

1. gather relevant Snowflake endpoints, for which you'll collect the CRL endpoints
   Issue `SELECT SYSTEM$ALLOWLIST()` (or `SYSTEM$ALLOWLIST_PRIVATELINK()` in a privatelink setup), and take a note of hosts for
* `SNOWFLAKE_DEPLOYMENT`
* `STAGE`

2. using the hostnames from 1., retrieve the certificate chain associated with those hostnames, and the relevant CRL endpoints for those server and intermediate certificates
   Again, this is just merely an example using shell scripting and `openssl`.
```shell
export hostname="myaccount.eu-central-1.snowflakecomputing.com"
echo | openssl s_client -showcerts -connect "$hostname":443 -servername "$hostname" 2>/dev/null | awk '/BEGIN/,/END/{ if(/BEGIN/){a++}; out="cert"a".pem"; print >out}'; for cert in cert*.pem; do echo "--> $cert"; openssl x509 -text -in $cert | grep -A4 "X509v3 CRL Distribution Points" ; echo; done
```

Example output:
```shell
--> cert1.pem
            X509v3 CRL Distribution Points:

                Full Name:
                  URI:http://www.microsoft.com/pkiops/crl/Microsoft%20Azure%20RSA%20TLS%20Issuing%20CA%2007.crl


--> cert2.pem
            X509v3 CRL Distribution Points:

                Full Name:
                  URI:http://crl3.digicert.com/DigiCertGlobalRootG2.crl


--> cert3.pem
            X509v3 CRL Distribution Points:

                Full Name:
                  URI:http://crl.pki.goog/gsr1/gsr1.crl
```

Please repeat the same for all the Snowflake-related endpoints from step 1.

For Windows if you do not wish to download additional tools, you can also use the existing Powershell facility. Please find the below Powershell script as a simplistic example of a possible approach. In this example, you would put below contents into `checkCrl.ps1` script:
```ps
if ( $($args.Count) -ne 1 ) {
	Write-Output "Please use the full name of your Snowflake account as an argument."
	Write-Output "Example: powershell .\checkCrl.ps1 xy12345.eu-central-1.snowflakecomputing.com"
	exit 1
}
$sfaccount = $args[0]  
$Connection = [System.Net.HttpWebRequest]::Create('https://' + $sfaccount)  
$Response = $Connection.GetResponse()  
$Response.Dispose()  
$Certificate = $Connection.ServicePoint.Certificate  
$Chain = New-Object -TypeName System.Security.Cryptography.X509Certificates.X509Chain  
$Chain.build($Certificate)  
$Chain.ChainElements.Certificate | % {set-content -value $($_.Export([Security.Cryptography.X509Certificates.X509ContentType]::Cert)) -encoding byte -path "$pwd\$($_.Thumbprint).sf.cer"}  
Get-ChildItem *.sf.cer | ForEach-Object { certutil $_ | Select-String -Pattern "Subject:" -Context 1 ; certutil $_ | Select-String -Pattern "Distribution Point Name" -Context 2 }
Remove-Item *.sf.cer
```

After saving it, you can run it with specifying your Snowflake account's full name. An example execution and output, for a Snowflake account located in GCP US Central region:
```shell
c:\temp>powershell .\checkCrl.ps1 xy12345.us-central1.gcp.snowflakecomputing.com
True


> Subject:
      CN=DigiCert Global G2 TLS RSA SHA256 2020 CA1
      CRL Distribution Points
          [1]CRL Distribution Point
>              Distribution Point Name:
                    Full Name:
                         URL=http://crl3.digicert.com/DigiCertGlobalRootG2.crl

> Subject:
      CN=*.us-central1.gcp.snowflakecomputing.com
      CRL Distribution Points
          [1]CRL Distribution Point
>              Distribution Point Name:
                    Full Name:
                         URL=http://crl3.digicert.com/DigiCertGlobalG2TLSRSASHA2562020CA1-1.crl
          [2]CRL Distribution Point
>              Distribution Point Name:
                    Full Name:
                         URL=http://crl4.digicert.com/DigiCertGlobalG2TLSRSASHA2562020CA1-1.crl

> Subject:
      CN=DigiCert Global Root G2
```

Look for values of `URL` fields under `Distribution Point Name` sections.

3. **Ensure (or work with your systems / network / cloud team to ensure) the CRL endpoints from step 2 are reachable from the _same host/network, over port 80_, on which host/network your application is running, which application is using the Snowflake .NET driver**

If your network includes any proxies through which the connection is sent, do make sure those proxies allow the connectivity to the CRL endpoints over port 80.
If those proxies also employ **SSL inspection technologies**, you'll need to configure them to **bypass the Snowflake-related endpoints** and not use SSL inspection, as [it is not supported in Snowflake](https://docs.snowflake.com/en/user-guide/client-connectivity-troubleshooting/common-issues#firewall-or-proxy-ssl-inspection-issues).

### Verifying connectivity to CRL endpoints
After retrieving the relevant CRL endpoints and configuring your environment to allow the revocation lists to be retrieved over port 80,
you can verify the reachability by simply trying to download the CRL from them.
**It is important to conduct the tests from the same host/network where the .NET driver will be used**.

Using the last CRL as an example from the above output:

-using `curl`:
```shell
curl -O http://crl.pki.goog/gsr1/gsr1.crl
# same with using a proxy:
curl -x "http://@my.pro.xy:8080" http://crl.pki.goog/gsr1/gsr1.crl
```
-using `wget`:
```shell
wget http://crl.pki.goog/gsr1/gsr1.crl
# same with using a proxy:
wget -e http_proxy=my.pro.xy:8080 http://crl.pki.goog/gsr1/gsr1.crl
```

Expected result is for the file to be successfully downloaded to the local filesystem.
Please make sure to **use the same proxy settings** as you will be using in your environment with the .NET driver.

### (break-glass) turning off CRL validation temporarily
**Snowflake does not recommend using it as a permanent configuration**, but for short periods of testing or perhaps getting out of a blocker situation, one can configure the .NET driver to not use CRL verification.
The steps are outlined in the [How To: Turn Off OCSP Checking in Snowflake Client Drivers](https://community.snowflake.com/s/article/How-to-turn-off-OCSP-checking-in-Snowflake-client-drivers) article, and basically is adding `insecuremode=true` to your connection string.

### DigiCert Global Root G2 certificate authority (CA) TLS certificate updates
This might or might not affect your installation. Since the .NET driver doesn't come with its own truststore, it depends on the system's own truststore,
which (hopefully) already includes all the root certificates needing to verify the chain of trust for connecting to Snowflake services.
If your installation is very old, this might not be the case. Please give the [FAQ: DigiCert Global Root G2 certificate authority (CA) TLS certificate updates](https://community.snowflake.com/s/article/check-impact-from-digicert-g2-certificate-update) article a read
on the background and possibly necessary steps.
