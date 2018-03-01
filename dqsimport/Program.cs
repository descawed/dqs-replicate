using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using Microsoft.Ssdqs.Studio.Infra.DataObjects;
using Microsoft.Ssdqs.Studio.ViewModels.Data.Common;
using Microsoft.Ssdqs.Studio.ViewModels.Data.DataSources;
using Microsoft.Ssdqs.Studio.ViewModels.Data.DomainRules;
using Microsoft.Ssdqs.Studio.ViewModels.Data.Domains;
using Microsoft.Ssdqs.Studio.ViewModels.Data.ReferenceDataServiceProviders;
using Microsoft.Ssdqs.Studio.ViewModels.Data.Settings;
using KnowledgebaseActivity = Microsoft.Ssdqs.Studio.ViewModels.Data.Common.KnowledgebaseActivity;

namespace dqsimport{
    internal static class Program{
        private struct Arguments{
            public string Instance;
            public bool Secure;
            public bool Verbose;
            public bool Quiet;
            public string FileName;
        }

        private struct CompositeDomainBookmark{
            public long Id;
            public long StreamPosition;
        }
        
        private static readonly AutoResetEvent[] ImportSync = {
            new AutoResetEvent(false), new AutoResetEvent(false)
        };

        private const int Success = 0;
        private const int Failure = 1;

        private static Arguments _config;
        
        private const int ChunkSize = 1024*1024*10; // 10MiB at a time
        private const int Magic = 0x01534a0a;

        private static long _result = -1;
        private static long _nextId = -1;
        private static long _nextDomainId = -1;

        private static KnowledgebaseDataProvider _dataProvider;
        
        public static void Main(string[] args){
            try{
                _config = ParseCommandLine(args);
                
                DataManager.Initialize();
                DataManager.ConnectToDatabase(_config.Instance, "DQS_MAIN", _config.Secure);

                _dataProvider =
                    (KnowledgebaseDataProvider) DataManager.GetProvider(DataProvider.KnowledgebaseDataProvider);

                ImportKnowledgebases();
                
                LogInfo("DQS instance successfully replicated");
            } catch (Exception e){
                FatalError("{0}", e.Message);
            }
        }

        private static void ImportKnowledgebases(){
            using (var reader = new BinaryReader(File.OpenRead(_config.FileName))){
                ReadHeader(reader);
                
                var numKbs = reader.ReadInt32();
                LogVerbose("Importing {0} knowledge bases from {1}", numKbs, _config.FileName);
                
                for (var i = 0; i < numKbs; i++){
                    var id = reader.ReadInt64();
                    var name = reader.ReadString();
                    var description = reader.ReadString();
                    var perDomain = reader.ReadBoolean();

                    if (perDomain){
                        var numDomains = reader.ReadInt32();
                        var compositeDomains = new List<CompositeDomainBookmark>();

                        CreateEmptyKnowledgebase(id, name, description);
                        for (var j = 0; j < numDomains; j++){
                            var domainId = reader.ReadInt64();
                            var isComposite = reader.ReadBoolean();
                            var size = reader.ReadInt64();

                            if (isComposite){
                                // defer import until afterwards to make sure all referenced domains exist
                                var startPos = reader.BaseStream.Position;
                                var domainName = reader.ReadString();
                                var domainDescription = reader.ReadString();
                                var parsingMethod = (CompositeDomainParsingMethod) reader.ReadInt32();
                                string delimiter = null;
                                if (parsingMethod == CompositeDomainParsingMethod.DelimiterParsing)
                                    delimiter = reader.ReadString();
                                
                                CreateEmptyCompositeDomain(id, domainId, domainName, domainDescription, parsingMethod,
                                    delimiter);
                                compositeDomains.Add(new CompositeDomainBookmark{
                                    Id = domainId,
                                    StreamPosition = reader.BaseStream.Position
                                });

                                reader.BaseStream.Seek(startPos + size, SeekOrigin.Begin);
                            } else{
                                var path = ExtractFile(reader, size);
                            
                                LogVerbose("Importing domain {0} for KB {1} with ID {2}", domainId, name, id);
                                ImportDomain(id, domainId, path);
                            
                                File.Delete(path);
                            }
                        }
                        
                        // import composite domains that were deferred
                        var kbEnd = reader.BaseStream.Position;
                        foreach (var cd in compositeDomains){
                            LogVerbose("Importing composite domain {0} for KB {1} with ID {2}", cd.Id, name, id);
                            ImportCompositeDomain(id, reader, cd);
                        }

                        reader.BaseStream.Seek(kbEnd, SeekOrigin.Begin);

                        PublishKnowledgebase(id);
                    } else{
                        var size = reader.ReadInt64();
                        var path = ExtractFile(reader, size);
                        
                        ImportFullKnowledgebase(id, name, description, path);
                        
                        File.Delete(path);
                    }
                }
            }
        }

        private static void ReadHeader(BinaryReader reader){
            if (reader.ReadInt32() != Magic)
                FatalError("Invalid or corrupt DQS export file");
            
            LogVerbose("Importing general settings...");

            var settingsProvider =
                (GeneralSettingsProvider) DataManager.GetProvider(DataProvider.GeneralSettingsProvider);
            var referenceDataProvider =
                (ReferenceDataProvider) DataManager.GetProvider(DataProvider.ReferenceDataProvider);

            var networkSettings = settingsProvider.GetNetworkConfiguration();
            networkSettings.ProxyServer = reader.ReadString();
            networkSettings.ProxyPort = reader.ReadBoolean() ? (int?)reader.ReadInt32() : null;
            settingsProvider.SaveNetworkConfiguration(networkSettings);

            var dallas = referenceDataProvider.GetReferenceDataServiceProviders().First(p => p.IsDallasProvider);

            // FIXME: check if the providers already exist
            var numProviders = reader.ReadInt32();
            for (var i = 0; i < numProviders; i++){
                var isDallas = reader.ReadBoolean();
                var accountId = reader.ReadString();
                var isActive = reader.ReadBoolean();

                if (isDallas){
                    dallas.AccountId = accountId;
                    dallas.IsActive = isActive;
                    referenceDataProvider.UpdateReferenceDataServiceProvider(dallas);
                    continue;
                }

                var name = reader.ReadString();
                var description = reader.ReadString();
                var category = reader.ReadString();
                if (category == "")
                    category = null;
                var maxBatchSize = reader.ReadInt32();
                var isFromCatalog = reader.ReadBoolean();
                var uri = reader.ReadString();

                var numSchema = reader.ReadInt32();
                var schema = new List<ReferenceDataSchema>(numSchema);
                for (var j = 0; j < numSchema; j++)
                    schema.Add(new ReferenceDataSchema(reader.ReadString(), reader.ReadBoolean()));

                referenceDataProvider.AddReferenceDataServiceProvider(new ReferenceDataServiceProvider{
                    AccountId = accountId,
                    Category = category,
                    Description = description,
                    IsActive = isActive,
                    IsDallasProvider = false,
                    IsFromDallasCatalog = isFromCatalog,
                    MaxBatchSize = maxBatchSize,
                    UniformResourceIdentifierAsString = uri,
                    ServiceProviderName = name,
                    Schema = schema
                });
            }

            var cleanseSettings = settingsProvider.GetInteractiveCleansingConfiguration();
            cleanseSettings.MinimalSuggestionScore = reader.ReadSingle();
            cleanseSettings.MinimalCorrectionScore = reader.ReadSingle();
            settingsProvider.SaveInteractiveCleansingConfiguration(cleanseSettings);

            var matchSettings = settingsProvider.GetMatchingConfiguration();
            matchSettings.MinimalRecordScore = reader.ReadInt32();
            settingsProvider.SaveMatchingConfiguration(matchSettings);

            var profileSettings = settingsProvider.GetProfilingConfiguration();
            profileSettings.DisableNotifications = reader.ReadBoolean();
            settingsProvider.SaveProfilingConfiguration(profileSettings);

            var numModules = reader.ReadInt32();
            var moduleSeverity = new Dictionary<LogModule, LogSeverity>(numModules);
            for (var i = 0; i < numModules; i++){
                var key = (LogModule)reader.ReadInt32();
                var severity = (LogSeverity) reader.ReadInt32();
                moduleSeverity[key] = severity;
            }

            var newModules = from moduleType in LogConfiguration.ModuleTypes
                select new LogConfigurationModule{
                    ModuleType = moduleType,
                    Severity = moduleSeverity[moduleType]
                };

            var logSettings = settingsProvider.GetLogConfiguration();
            logSettings.Modules = newModules;

            var numNamespaces = reader.ReadInt32();
            for (var i = 0; i < numNamespaces; i++){
                var key = reader.ReadString();
                var value = (LogSeverity) reader.ReadInt32();
                logSettings.Namespaces[key] = value;
            }

            settingsProvider.SaveLogConfiguration(logSettings);
        }

        private static void ImportDomain(long kbId, long domainId, string path){
            var kb = AcquireKnowledgebase(kbId);
            
            var domainProvider = (DomainProvider) DataManager.GetProvider(DataProvider.DomainProvider);
            domainProvider.Knowledgebase = kb;
            

            if (_nextDomainId == -1){
                var currentId = ImportDomainAndWait(kb, path);

                if (currentId == domainId)
                    return;
                
                domainProvider.DeleteDomain(domainProvider.GetDomain(currentId));

                if (currentId > domainId)
                    FatalError("Attempted to create domain with ID {0} but the current ID is already {1}", domainId,
                        currentId);
            }
            
            AdvanceToDomainId(domainProvider, domainId);
            if (ImportDomainAndWait(kb, path) != domainId)
                FatalError("Failed to acquire domain ID {0} (this is probably a bug)", domainId);
        }

        private static void ImportCompositeDomain(long kbId, BinaryReader reader, CompositeDomainBookmark bookmark){
            var kb = AcquireKnowledgebase(kbId);
            
            var domainProvider = (DomainProvider) DataManager.GetProvider(DataProvider.DomainProvider);
            domainProvider.Knowledgebase = kb;
            
            var referenceDataProvider =
                (ReferenceDataProvider) DataManager.GetProvider(DataProvider.ReferenceDataProvider);
            referenceDataProvider.Knowledgebase = kb;
            var allProviders = referenceDataProvider.GetReferenceDataServiceProviders()
                .ToDictionary(p => p.ServiceProviderName, p => p);
            
            var ruleProvider = (DomainRuleProvider) DataManager.GetProvider(DataProvider.DomainRuleProvider);
            ruleProvider.Knowledgebase = kb;

            var domain = (CompositeDomain) domainProvider.GetDomain(bookmark.Id);
            reader.BaseStream.Seek(bookmark.StreamPosition, SeekOrigin.Begin);

            var formatter = new BinaryFormatter();

            // linked domains
            var numLinkedDomains = reader.ReadInt32();
            var linkedDomains = new ElementCollection<Domain>();
            for (var i = 0; i < numLinkedDomains; i++)
                linkedDomains.Add((Domain) domainProvider.GetDomain(reader.ReadInt64()));

            domain.LinkedDomains = linkedDomains;
            domain = (CompositeDomain) domainProvider.SaveDomainChanges(domain);
            
            // reference data providers
            var numRefDataProviders = reader.ReadInt32();
            for (var i = 0; i < numRefDataProviders; i++){
                var referenceProvider = allProviders[reader.ReadString()];
                var correctionThreshold = reader.ReadSingle();
                var suggestedCandidates = reader.ReadInt32();
                var minConfidence = reader.ReadSingle();

                var childSchema = new Dictionary<DomainBase, string>();
                var numChildSchema = reader.ReadInt32();
                for (var j = 0; j < numChildSchema; j++){
                    var childDomain = domainProvider.GetDomain(reader.ReadInt64());
                    var value = reader.ReadString();
                    childSchema.Add(childDomain, value);
                }

                var attachedProvider = new AttachedReferenceDataProvider{
                    Provider = referenceProvider,
                    ChildDomainSchemaChanged = true,
                    ChildDomainsSchema = childSchema,
                    CorrectionThreshold = correctionThreshold,
                    SuggestedCandidates = suggestedCandidates,
                    MinimalConfidence = minConfidence,
                    Domain = domain,
                    AdditionalParameters = new NameValueCollection()
                };
                
                referenceDataProvider.AttachReferenceDataProviderToDomain(attachedProvider);
            }
            
            // CD rules
            var numRules = reader.ReadInt32();
            long nextRuleId = -1;
            for (var i = 0; i < numRules; i++){
                var ruleId = reader.ReadInt64();
                var name = reader.ReadString();
                var description = reader.ReadString();
                var isActive = reader.ReadBoolean();
                var createdBy = reader.ReadString();
                var lastUpdated = DateTime.FromBinary(reader.ReadInt64());

                var numConditions = reader.ReadInt32();
                var conditions = new ElementCollection<CompositeDomainRuleClause>();
                for (var j = 0; j < numConditions; j++){
                    var domainId = reader.ReadInt64();
                    var op = (DomainRuleClauseLogicalOperation) reader.ReadInt32();
                    var clauses = ImportClauses(reader, formatter);
                    
                    conditions.Add(new CompositeDomainRuleClause{
                        Clauses = clauses,
                        DomainId = domainId,
                        LogicalOperation = op
                    });
                }

                var conclusionDomain = reader.ReadInt64();
                var conclusionOp = (DomainRuleClauseLogicalOperation) reader.ReadInt32();
                var conclusionClauses = ImportClauses(reader, formatter);
                var conclusion = new CompositeDomainRuleClause{
                    Clauses = conclusionClauses,
                    DomainId = conclusionDomain,
                    LogicalOperation = conclusionOp
                };

                var rule = new CompositeDomainRule{
                    Conclusion = conclusion,
                    Conditions = conditions,
                    CreatedBy = createdBy,
                    Description = description,
                    DomainId = bookmark.Id,
                    IsActive = isActive,
                    LastUpdate = lastUpdated,
                    Name = name
                };
                if (CreateRuleWithId(ruleProvider, rule, ruleId, nextRuleId) != ruleId)
                    FatalError("Failed to acquire rule ID {0} (this is probably a bug)", ruleId);
                nextRuleId = ruleId + 1;
            }
        }

        private static ElementCollection<DomainRuleClause> ImportClauses(BinaryReader reader, IFormatter formatter){
            var numClauses = reader.ReadInt32();
            var clauses = new ElementCollection<DomainRuleClause>();
            for (var i = 0; i < numClauses; i++){
                var op = (DomainRuleClauseOperator) reader.ReadInt32();
                var value = formatter.Deserialize(reader.BaseStream);
                var logicalOp = (DomainRuleClauseLogicalOperation) reader.ReadInt32();
                var subClauses = ImportClauses(reader, formatter);

                clauses.Add(new DomainRuleClause{
                    Clauses = subClauses,
                    LogicalOperation = logicalOp,
                    Operator = op,
                    Value = value
                });
            }

            return clauses;
        }

        private static long CreateRuleWithId(DomainRuleProvider provider, DomainRuleBase rule, long id, long nextId){
            if (nextId == -1){
                var newRule = (DomainRuleBase) provider.AddDomainRule(rule).DataObject;
                nextId = newRule.Id + 1;

                if (newRule.Id == id)
                    return newRule.Id;
                
                provider.DeleteRule(newRule);
                
                if (newRule.Id > id)
                    FatalError("Attempted to create rule with ID {0} but the current ID is already {1}", id,
                        newRule.Id);
            }

            while (nextId < id){
                var newRule = (DomainRuleBase) provider.AddDomainRule(rule).DataObject;
                nextId = newRule.Id + 1;
                provider.DeleteRule(newRule);
            }

            var finalRule = (DomainRuleBase) provider.AddDomainRule(rule).DataObject;
            return finalRule.Id;
        }

        private static void AdvanceToDomainId(DomainProvider provider, long id){
            while (_nextDomainId < id){
                var domain = new Domain{
                    Name = "$unused$",
                    DataType = DomainDataType.String
                };
                var realDomain = provider.NewDomain(domain);
                _nextDomainId = realDomain.Id + 1;
                provider.DeleteDomain(realDomain);
            }
        }

        private static long ImportDomainAndWait(Knowledgebase kb, string path){
            var provider = (ImportExportProvider) DataManager.GetProvider(DataProvider.ImportExportDataProvider);
            provider.Knowledgebase = kb;
            provider.ImportDomain(path, ImportComplete);
            // ReSharper disable once CoVariantArrayConversion
            if (WaitHandle.WaitAny(ImportSync) == Failure)
                FatalError("Domain import for KB {0} failed", kb.Id);

            if (_result != -1)
                _nextDomainId = _result + 1;

            return _result;
        }

        private static void CreateEmptyCompositeDomain(long kbId, long id, string name, string description,
            CompositeDomainParsingMethod parsingMethod, string delimiter){
            var kb = AcquireKnowledgebase(kbId);
            
            var domainProvider = (DomainProvider) DataManager.GetProvider(DataProvider.DomainProvider);
            domainProvider.Knowledgebase = kb;
            
            // DQS won't let you create a composite domain without at least two linked domains, so we'll try
            // to find some dummy domains to throw in there until all the regular domains are imported
            var linkedDomains = new ElementCollection<Domain>();
            foreach (var domain in domainProvider.GetDomains()){
                if (domain is Domain d)
                    linkedDomains.Add(d);
                if (linkedDomains.Count >= 2)
                    break;
            }
            
            // FIXME: the only way this could happen is if all the lower domains were deleted, meaning there are at
            // least two free IDs, so create some dummy domains to throw in here and delete them at the end
            if (linkedDomains.Count < 2)
                FatalError("Not enough domains to create composite domain; import impossible");
            
            if (_nextDomainId == -1){
                var cd = new CompositeDomain{
                    Name = name,
                    Description = description,
                    ParsingMethod = parsingMethod,
                    OtherDelimiter = delimiter?? " ",
                    LinkedDomains = linkedDomains
                };

                var realCd = domainProvider.NewCompositeDomain(cd);
                _nextDomainId = realCd.Id + 1;

                if (realCd.Id == id)
                    return;
                
                domainProvider.DeleteDomain(realCd);
                
                if (realCd.Id > id)
                    FatalError("Attempted to create domain with ID {0} but the current ID is already {1}", id,
                        realCd.Id);
            }
            
            AdvanceToDomainId(domainProvider, id);
            
            var newCd =  new CompositeDomain{
                Name = name,
                Description = description,
                ParsingMethod = parsingMethod,
                OtherDelimiter = delimiter?? " ",
                LinkedDomains = linkedDomains
            };
            var createdCd = domainProvider.NewCompositeDomain(newCd);
            if (createdCd.Id != id)
                FatalError("Failed to acquire domain ID {0} (this is probably a bug)", id);
            _nextDomainId = createdCd.Id + 1;
        }

        private static void CreateEmptyKnowledgebase(long id, string name, string description){
            if (_nextId == -1){
                var kb = new Knowledgebase{
                    Name = name,
                    Description = description
                };

                var realKb = _dataProvider.CreateKnowledgebase(kb, KnowledgebaseActivity.DomainManagement);
                _nextId = realKb.Id + 1;

                if (realKb.Id == id)
                    return;
                
                _dataProvider.DeleteKnowledgebase(realKb);
                
                if (realKb.Id > id)
                    FatalError("Attempted to create knowledgebase with ID {0} but the current ID is already {1}", id,
                        realKb.Id);
            }
            
            AdvanceToKnowledgebaseId(id);
            var newKb = new Knowledgebase{
                Name = name,
                Description = description
            };
            var createdKb = _dataProvider.CreateKnowledgebase(newKb, KnowledgebaseActivity.DomainManagement);
            if (createdKb.Id != id)
                FatalError("Failed to acquire knowledgebase ID {0} (this is probably a bug)", id);
            _nextId = createdKb.Id + 1;
            _nextDomainId = -1;
        }

        private static void ImportFullKnowledgebase(long id, string name, string description, string path){
            LogVerbose("Importing KB {0} at ID {1}...", name, id);
            
            if (_nextId == -1){
                // we don't know what the next ID will be, so we have no choice but to import the KB once to see
                var currentId = ImportKnowledgebaseAndWait(name, description, path);

                // if the ID we got is the one we wanted, we're done
                if (currentId == id)
                    return;
                
                // otherwise, delete the extraneous KB we just created
                var kb = _dataProvider.GetKnowledgebaseById(currentId);
                _dataProvider.DeleteKnowledgebase(kb);

                if (currentId > id)
                    FatalError("Attempted to create knowledgebase with ID {0} but the current ID is already {1}", id,
                        currentId);
            }

            AdvanceToKnowledgebaseId(id);
            if (ImportKnowledgebaseAndWait(name, description, path) != id)
                FatalError("Failed to acquire knowledgebase ID {0} (this is probably a bug)", id);
            
            // publish and unlock the knowledge base we just created
            PublishKnowledgebase(id);
        }

        private static void PublishKnowledgebase(long id){
            var kb = _dataProvider.GetKnowledgebaseById(id);
            kb = _dataProvider.OpenKnowledgebase(kb, KnowledgebaseActivity.DomainManagement);
            _dataProvider.PublishKnowledgebase(kb);
        }

        private static void AdvanceToKnowledgebaseId(long id){
            // create and delete knowledge bases until we get to the requisite ID
            while (_nextId < id){
                var kb = new Knowledgebase{
                    Name = "$unused$"
                };
                var realKb = _dataProvider.CreateKnowledgebase(kb, KnowledgebaseActivity.DomainManagement);
                _nextId = realKb.Id + 1;
                _dataProvider.DeleteKnowledgebase(realKb);
            }
        }

        private static long ImportKnowledgebaseAndWait(string name, string description, string path){
            var provider = (ImportExportProvider) DataManager.GetProvider(DataProvider.ImportExportDataProvider);
            provider.ImportKnowledgebase(name, description, path, ImportComplete);
            // ReSharper disable once CoVariantArrayConversion
            if (WaitHandle.WaitAny(ImportSync) == Failure)
                FatalError("Knowledge base {0} import failed", name);

            if (_result != -1)
                _nextId = _result + 1;
            
            return _result;
        }

        private static Knowledgebase AcquireKnowledgebase(long id){
            var kb = _dataProvider.GetKnowledgebaseById(id);
            if (!kb.IsLockedByCurrentUser)
                kb = _dataProvider.OpenKnowledgebase(kb, KnowledgebaseActivity.DomainManagement);

            return kb;
        }

        private static string ExtractFile(BinaryReader reader, long size){
            var path = GetTempFileName();

            using (var writer = new BinaryWriter(File.Create(path))){
                for (long bytesRead = 0; bytesRead < size; bytesRead += ChunkSize){
                    var bytesLeft = size - bytesRead;
                    var toRead = (int) (bytesLeft < ChunkSize ? bytesLeft : ChunkSize);
                    writer.Write(reader.ReadBytes(toRead));
                }
            }

            return path;
        }
        
        private static void LogVerbose(string message, params object[] args){
            if (_config.Verbose)
                Console.WriteLine(message, args);
        }
        
        private static void LogInfo(string message, params object[] args){
            if (!_config.Quiet)
                Console.WriteLine(message, args);
        }
        
        private static Arguments ParseCommandLine(string[] args){
            var config = new Arguments{
                Instance = "(LOCAL)",
                Secure = false,
                Verbose = false,
                Quiet = false,
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
                    config.FileName = arg;
                }
            }

            if (config.FileName == null)
                FatalError("No import filename provided");

            return config;
        }
        
        private static void ImportComplete(object obj, Exception exception){
            if (exception != null){
                Console.Error.WriteLine("Error while importing {0}: {1}", obj, exception);
                _result = -1;
                ImportSync[Failure].Set();
            } else{
                switch (obj){
                    case Knowledgebase kb:
                        _result = kb.Id;
                        break;
                    case CompositeDomain cd:
                        _result = cd.Id;
                        break;
                    case Domain d:
                        _result = d.Id;
                        break;
                    default:
                        _result = -1;
                        break;
                }

                ImportSync[Success].Set();
            }
        }
        
        private static string GetTempFileName(){
            return Path.GetTempPath() + Guid.NewGuid() + ".dqs";
        }

        private static void Help(){
            Console.WriteLine("Usage: dqsimport [-instance <name>] [-verbose] [-quiet] [-secure] [-help] <file>");
            Console.WriteLine("");
            Console.WriteLine("\t-instance <name>\tConnect to DQS instance <name>. Default is (LOCAL).");
            Console.WriteLine("\t-verbose\t\tPrint additional status information.");
            Console.WriteLine("\t-quiet\t\t\tNo confirmations or output except error output. Overrides -verbose.");
            Console.WriteLine("\t-secure\t\t\tUse a secure connection to DQS.");
            Console.WriteLine("\t-help\t\t\tPrint this help message and exit.");
            Console.WriteLine("\t<file>\t\t\tPath to an export file created by dqsexport.");
            Environment.Exit(0);
        }

        private static void FatalError(string message, params object[] args){
            Console.Error.WriteLine("Fatal error: " + message, args);
            Environment.Exit(1);
        }
    }
}