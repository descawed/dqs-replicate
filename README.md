# DQS Replication

This is a set of two utility programs for replicating a Data Quality Services setup to another instance. The DQS SSIS components reference knowledge bases by their IDs, but the built-in
options for exporting and importing KBs and domains, whether exporting one at a time through the DQS client or doing a wholesale export with dqsinstaller -exportkbs, do not preserve the
IDs. This makes setting up a development DQS instance (among other use cases) a frustrating experience. The utilities provided here, dqsexport and dqsimport, attempt to replicate your DQS
knowledge bases and domains with the same IDs, as well as your DQS configuration (but not data quality projects).

## Requirements

* SQL Server 2014 SP2 or SQL Server 2017. I assume 2016 would work too, but I haven't tested that one.
* For exporting: [this patch](https://support.microsoft.com/en-us/help/4022483/fix-error-when-you-export-a-dqs-knowledge-base-that-contains-domains).
* For importing: a fresh DQS installation containing only the out-of-the-box "DQS Data" knowledge base.

## Usage

1. Drop dqsexport.exe and dqsimport.exe in C:\Program Files (x86)\Microsoft SQL Server\nnn\Tools\Binn\DQ, where nnn corresponds to your SQL Server version (2014 = 120, 2017 = 140)
2. Run dqsexport on the instance you want to replicate to create an export file. See dqsexport -help for options.
3. For local DQS connections, send the export file to the machine where the new DQS instance lives.
4. Run dqsimport on the file to replicate the settings and knowledge bases. See dqsimport -help for options.

## Known Issues

* Data quality projects are not replicated. I'm not sure if this would be useful as I've used DQS exclusively through SSIS, where the endless projects it creates are mostly a nuisance.
* No clean up is performed on error. This may result in the import of incomplete knowledge bases and/or files accumulating in your temp directory.
* The import can take a very long time if you have large gaps in your KB IDs. I may look into reseeding the table in between KBs as an option to eliminate this overhead.
* To my knowledge, the DQS APIs are not intended for public use and may undergo breaking changes at any time.
* The code is pretty sloppy. I'm not much of a .NET guy and I initially kind of threw this together because I thought it would be a simple project. I may do a v2 and clean things up.