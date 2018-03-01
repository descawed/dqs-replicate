using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using Microsoft.Ssdqs.Studio.ViewModels.Data.Common;
using Microsoft.Ssdqs.Studio.ViewModels.Data.DomainRules;
using Microsoft.Ssdqs.Studio.ViewModels.Data.Domains;
using Microsoft.Ssdqs.Studio.ViewModels.Data.ReferenceDataServiceProviders;
using Microsoft.Ssdqs.Studio.ViewModels.Data.Settings;

namespace dqsexport{
    internal static class Program{
        private struct Arguments{
            public string Instance;
            public bool Secure;
            public bool Verbose;
            public bool Quiet;
            public string FileName;
            public List<string> Exclusions;
        }

        private struct DomainEntry{
            public long Id;
            public bool IsComposite;
            public string FileName;
        }

        private struct KnowledgebaseEntry{
            public long Id;
            public string Name;
            public string Description;
            public bool PerDomain;
            public string FileName;
            public List<DomainEntry> Domains;
        }

        private static readonly AutoResetEvent[] ExportSync = {
            new AutoResetEvent(false), new AutoResetEvent(false)
        };

        private const int Success = 0;
        private const int Failure = 1;
        
        
        private static Arguments _config;
        
        private const int ChunkSize = 1024*1024*10; // 10MiB at a time
        private const int Magic = 0x01534a0a;
        
        public static void Main(string[] args){
            try{
                _config = ParseCommandLine(args);

                DataManager.Initialize();
                DataManager.ConnectToDatabase(_config.Instance, "DQS_MAIN", _config.Secure);

                // TODO: offer to create the directory if it doesn't exist
                var path = Path.GetDirectoryName(_config.FileName);
                if (!Directory.Exists(path))
                    FatalError("Path {0} does not exist", path);
                
                if (File.Exists(_config.FileName) && !_config.Quiet){
                    Console.Write("Destination file exists. Overwrite? [y/n] ");
                    var answer = (Console.ReadLine() ?? "n").ToLower();
                    if (answer != "y"){
                        Console.WriteLine("Aborting export");
                        Environment.Exit(0);
                    }
                }

                var dataProvider =
                    (KnowledgebaseDataProvider) DataManager.GetProvider(DataProvider.KnowledgebaseDataProvider);
                var entries = new List<KnowledgebaseEntry>();
                foreach (var kb in dataProvider.GetKnowledgebaseList()){
                    // skip KB if it was excluded
                    if (_config.Exclusions.Any(x =>
                        x.Equals(kb.Name, StringComparison.OrdinalIgnoreCase) || x == kb.Id.ToString()))
                        continue;

                    dataProvider.OpenKnowledgebase(kb, KnowledgebaseActivity.DomainManagement);
                    try{
                        entries.Add(ExportKnowledgebase(kb));
                    }
                    finally{
                        // the export process counts as revision of the KB, so we have to re-fetch it to unlock it
                        dataProvider.UnlockKnowledgebase(dataProvider.GetKnowledgebaseById(kb.Id));
                    }
                }

                MakeArchive(_config.FileName, entries);

                LogInfo("Wrote {0} knowledge bases to {1}", entries.Count, _config.FileName);
            } catch (Exception e){
                FatalError("{0}", e.Message);
            }
        }

        private static void MakeArchive(string path, List<KnowledgebaseEntry> entries){
            using (var writer = new BinaryWriter(File.Create(path))){
                WriteHeader(writer);
                
                // number of knowledge bases - int
                writer.Write(entries.Count);

                foreach (var entry in entries){
                    // KB ID - long
                    writer.Write(entry.Id);
                    // KB name - string
                    writer.Write(entry.Name);
                    // KB description - string
                    writer.Write(entry.Description);
                    // one big KB file or file per domain - bool
                    writer.Write(entry.PerDomain);

                    if (entry.PerDomain){
                        // number of domains - int
                        writer.Write(entry.Domains.Count);

                        foreach (var domain in entry.Domains){
                            // domain ID - long
                            writer.Write(domain.Id);
                            // composite flag - bool
                            writer.Write(domain.IsComposite);

                            // file size - long
                            using (var reader = new BinaryReader(File.OpenRead(domain.FileName))){
                                writer.Write(reader.BaseStream.Length);
                                // file contents - byte[]
                                while (reader.BaseStream.Position < reader.BaseStream.Length)
                                    writer.Write(reader.ReadBytes(ChunkSize));
                            }
                            
                            File.Delete(domain.FileName);
                        }
                    } else{
                        using (var reader = new BinaryReader(File.OpenRead(entry.FileName))){
                            // file size - long
                            writer.Write(reader.BaseStream.Length);
                            // file contents - byte[]
                            while (reader.BaseStream.Position < reader.BaseStream.Length)
                                writer.Write(reader.ReadBytes(ChunkSize));
                        }
                        
                        File.Delete(entry.FileName);
                    }
                }
            }
        }

        private static void LogVerbose(string message, params object[] args){
            if (_config.Verbose && !_config.Quiet)
                Console.WriteLine(message, args);
        }

        private static void LogInfo(string message, params object[] args){
            if (!_config.Quiet)
                Console.WriteLine(message, args);
        }

        private static void WriteHeader(BinaryWriter writer){
            var referenceDataProvider =
                (ReferenceDataProvider) DataManager.GetProvider(DataProvider.ReferenceDataProvider);
            
            var settingsProvider =
                (GeneralSettingsProvider) DataManager.GetProvider(DataProvider.GeneralSettingsProvider);
            var networkSettings = settingsProvider.GetNetworkConfiguration();
            var matchSettings = settingsProvider.GetMatchingConfiguration();
            var profileSettings = settingsProvider.GetProfilingConfiguration();
            var cleanseSettings = settingsProvider.GetInteractiveCleansingConfiguration();
            
            // magic constant - int
            writer.Write(Magic);
            
            // configuration
            // reference data
            
            // proxy server - string
            writer.Write(networkSettings.ProxyServer);
            // has proxy port - bool
            var hasProxyPort = networkSettings.ProxyPort != null; 
            writer.Write(hasProxyPort);
            if (hasProxyPort)
                // proxy port - int
                writer.Write((int)networkSettings.ProxyPort);
            
            // number of providers - int
            writer.Write(referenceDataProvider.GetReferenceDataServiceProviders().Count());
            foreach (var provider in referenceDataProvider.GetReferenceDataServiceProviders()){
                // is default DataMarket provider - bool
                writer.Write(provider.IsDallasProvider);
                // account ID - string
                writer.Write(provider.AccountId);
                // is active - bool
                writer.Write(provider.IsActive);

                if (provider.IsDallasProvider)
                    continue;
                
                // name - string
                writer.Write(provider.ServiceProviderName);
                // description - string
                writer.Write(provider.Description);
                // category - string
                writer.Write(provider.Category?? "");
                // max batch size - int
                writer.Write(provider.MaxBatchSize);
                // from DataMarket - bool
                writer.Write(provider.IsFromDallasCatalog);
                // URI - string
                writer.Write(provider.UniformResourceIdentifierAsString);
                
                // schema
                // number of schema - int
                writer.Write(provider.Schema.Count);
                foreach (var schema in provider.Schema){
                    // name - string
                    writer.Write(schema.Schema);
                    // is mandatory - bool
                    writer.Write(schema.IsMandatory);
                }
            }
            
            // general settings
            
            // min score for suggestions - float
            writer.Write(cleanseSettings.MinimalSuggestionScore);
            // min score for auto corrections - float
            writer.Write(cleanseSettings.MinimalCorrectionScore);
            // min record score - int
            writer.Write(matchSettings.MinimalRecordScore);
            // disable notifications - bool
            writer.Write(profileSettings.DisableNotifications);
            
            // log settings
            
            // number of modules - int
            writer.Write(settingsProvider.GetLogConfiguration().Modules.Count());
            var logSettings = settingsProvider.GetLogConfiguration();
            foreach (var module in logSettings.Modules){
                // module type - int
                writer.Write((int)module.ModuleType);
                // log severity - int
                writer.Write((int)module.Severity);
            }
            
            // number of namespace log settings - int
            writer.Write(logSettings.Namespaces.Count);
            foreach (var pair in logSettings.Namespaces){
                // namespace - string
                writer.Write(pair.Key);
                // log severity - int
                writer.Write((int)pair.Value);
            }
        }

        private static void ExportCompositeDomain(Knowledgebase kb, CompositeDomain domain, string path){
            var referenceDataProvider =
                (ReferenceDataProvider) DataManager.GetProvider(DataProvider.ReferenceDataProvider);
            referenceDataProvider.Knowledgebase = kb;
            var refDataProviders = referenceDataProvider.GetAttachedReferenceDataProviders(domain);

            var ruleProvider = (DomainRuleProvider) DataManager.GetProvider(DataProvider.DomainRuleProvider);
            ruleProvider.Knowledgebase = kb;
            var rules = ruleProvider.GetDomainRules(domain);

            var formatter = new BinaryFormatter();

            using (var writer = new BinaryWriter(File.Create(path))){
                // name - string
                writer.Write(domain.Name);
                // description - string
                writer.Write(domain.Description?? "");
                // parsing method - int
                writer.Write((int)domain.ParsingMethod);
                // if the parsing method is delimiter, write the delimiter
                if (domain.ParsingMethod == CompositeDomainParsingMethod.DelimiterParsing)
                    // delimiter - string
                    writer.Write(domain.OtherDelimiter);
                
                // linked domains
                // number of linked domains - int
                writer.Write(domain.LinkedDomains.Count);
                foreach (var linkedDomain in domain.LinkedDomains)
                    // domain ID - long
                    writer.Write(linkedDomain.Id);
                
                // reference data providers
                // number of reference data providers - int
                writer.Write(refDataProviders.Count);
                foreach (var provider in refDataProviders){
                    // reference data provider name - string
                    writer.Write(provider.Provider.ServiceProviderName);
                    // auto correction threshold - float
                    writer.Write(provider.CorrectionThreshold);
                    // suggested candidates - int
                    writer.Write(provider.SuggestedCandidates);
                    // min confidence - float
                    writer.Write(provider.MinimalConfidence);
                    
                    // schema
                    // number of schema - int
                    writer.Write(provider.ChildDomainsSchema.Count);
                    foreach (var pair in provider.ChildDomainsSchema){
                        // domain ID - long
                        writer.Write(pair.Key.Id);
                        // schema name - string
                        writer.Write(pair.Value);
                    }
                }
                
                // CD rules
                // number of rules - int
                writer.Write(rules.Count);
                foreach (var rule in rules){
                    var cdRule = (CompositeDomainRule) rule;
                    
                    // rule ID - long
                    writer.Write(cdRule.Id);
                    // name - string
                    writer.Write(cdRule.Name);
                    // description - string
                    writer.Write(cdRule.Description?? "");
                    // active - bool
                    writer.Write(cdRule.IsActive);
                    // created by - string
                    writer.Write(cdRule.CreatedBy);
                    // last updated - long
                    writer.Write(cdRule.LastUpdate.ToBinary());
                    
                    // conditions
                    // number of conditions - int
                    writer.Write(cdRule.Conditions.Count);
                    foreach (var condition in cdRule.Conditions){
                        // domain ID - long
                        writer.Write(condition.DomainId);
                        // logical operation - int
                        writer.Write((int)condition.LogicalOperation);
                        
                        // clauses
                        ExportClauses(writer, formatter, condition.Clauses);
                    }
                    
                    // conclusion
                    // domain ID - long
                    writer.Write(cdRule.Conclusion.DomainId);
                    // logical operation - int
                    writer.Write((int)cdRule.Conclusion.LogicalOperation);
                    // clauses
                    ExportClauses(writer, formatter, cdRule.Conclusion.Clauses);
                }
            }
        }

        private static void ExportClauses(BinaryWriter writer, IFormatter formatter,
            ICollection<DomainRuleClause> clauses){
            // number of clauses - int
            writer.Write(clauses.Count);
            foreach (var clause in clauses){
                // operator - int
                writer.Write((int)clause.Operator);
                // value - serialized object
                formatter.Serialize(writer.BaseStream, clause.Value);
                // logical operation - int
                writer.Write((int)clause.LogicalOperation);
                
                ExportClauses(writer, formatter, clause.Clauses);
            }
        }

        private static KnowledgebaseEntry ExportKnowledgebase(Knowledgebase kb){
            var entry = new KnowledgebaseEntry{
                Id = kb.Id,
                Name = kb.Name,
                Description = kb.Description,
                PerDomain = false,
                FileName = null,
                Domains = new List<DomainEntry>()
            };
            
            LogVerbose("Processing KB {0} with ID {1}...", kb.Name, kb.Id);
            
            var domainProvider = (DomainProvider) DataManager.GetProvider(DataProvider.DomainProvider);
            domainProvider.Knowledgebase = kb;
            var domains = domainProvider.GetDomains();
            // always export by domain since domain IDs are not preserved otherwise
            entry.PerDomain = true;

            var exportProvider = (ImportExportProvider) DataManager.GetProvider(DataProvider.ImportExportDataProvider);
            exportProvider.Knowledgebase = kb;
            // knowledge bases with composite domains must be exported one domain at a time
            foreach (var domain in domains){
                var path = GetTempFileName();
                var domainEntry = new DomainEntry{
                    Id = domain.Id,
                    IsComposite = false,
                    FileName = path
                };

                if (domain is CompositeDomain cd){
                    domainEntry.IsComposite = true;
                    ExportCompositeDomain(kb, cd, path);
                } else{
                    exportProvider.ExportDomain(domain, path, ExportComplete);
                    // ReSharper disable once CoVariantArrayConversion
                    if (WaitHandle.WaitAny(ExportSync) == Failure)
                        FatalError("Domain export failed");
                }
                
                entry.Domains.Add(domainEntry);
            }

            return entry;
        }

        private static Arguments ParseCommandLine(string[] args){
            var config = new Arguments{
                Instance = "(LOCAL)",
                Secure = false,
                Verbose = false,
                Quiet = false,
                // always exclude the default DQS Data knowledgebase
                Exclusions = new List<string>{ "1000000" },
                FileName = null
            };

            for (var i = 0; i < args.Length; i++){
                var arg = args[i];
                if (arg[0] == '-' || arg[0] == '/'){
                    switch (arg.Substring(1)){
                        case "instance":
                            if (args.Length <= i+1)
                                FatalError("-instance flag supplied without instance name");
                            config.Instance = args[++i];
                            break;
                        case "secure":
                            config.Secure = true;
                            break;
                        case "verbose":
                            config.Verbose = true;
                            break;
                        case "quiet":
                            config.Quiet = true;
                            break;
                        case "exclude":
                            if (args.Length <= i+1)
                                FatalError("-exclude flag supplied without KB identifier");
                            config.Exclusions.Add(args[++i]);
                            break;
                        case "h":
                            goto case "help";
                        case "?":
                            goto case "help";
                        case "help":
                            Help();
                            break;
                        default:
                            Console.Error.WriteLine("Unexpected argument " + arg);
                            Help();
                            break;
                    }
                } else{
                    if (config.FileName != null)
                        FatalError("Multiple export files specified (did you forget to put an argument in quotes?)");
                    config.FileName = arg;
                }
            }

            if (config.FileName == null)
                FatalError("No export filename provided");

            return config;
        }
        
        private static void ExportComplete(object obj, Exception exception){
            if (exception != null){
                Console.Error.WriteLine("Error while exporting {0}: {1}", obj, exception);
                ExportSync[Failure].Set();
            } else{
                ExportSync[Success].Set();
            }
        }

        private static string GetTempFileName(){
            return Path.GetTempPath() + Guid.NewGuid() + ".dqs";
        }
        
        private static void Help(){
            Console.WriteLine("Usage: dqsexport <file> [-instance <name>] [-secure] [-exclude <name-or-id> ...] [-verbose] [-quiet] [-help]");
            Console.WriteLine("");
            Console.WriteLine("\t<file>\t\t\tPath where the export file will be created.");
            Console.WriteLine("\t-instance <name>\tConnect to DQS instance <name>. Default is (LOCAL).");
            Console.WriteLine("\t-secure\t\t\tUse a secure connection to DQS.");
            Console.WriteLine("\t-exclude\t\tName or ID of a knowledgebase to exclude from the export. Can be specified multiple times.");
            Console.WriteLine("\t-verbose\t\tPrint additional status information.");
            Console.WriteLine("\t-quiet\t\t\tNo confirmations or output except error output. Overrides -verbose.");
            Console.WriteLine("\t-help\t\t\tPrint this help message and exit.");
            Environment.Exit(0);
        }

        private static void FatalError(string message, params object[] args){
            Console.Error.WriteLine("Fatal error: " + message, args);
            Environment.Exit(1);
        }
    }
}