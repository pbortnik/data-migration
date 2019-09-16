Simple example of reading data from mongo database and storing it into Postgres. 


While importing projects and project_users relation, migration ignores non-existing users
assigned at the project, duplicate users, project without assigned users. Migration adds
"migrated_from" field in project metadata to mark that project was imported from MongoDB.

Projects are read from mongo by batch number and inserts one by one into postgres. Project users
and issue types are batched inserted into postgres per project. Attributes are batched inserted per project 
as well. Analyzer config properties "analyzer.minDocFreq = 1", "analyzer.minTermFreq = 1", "analyzer.minShouldMatch = 95", 
"analyzer.numberOfLogLines = 4". Email send rules are migrated per project.

Before migrating BTS, the Jira or Rally plugins should be loaded to the RP v5. If they are not found, nothing would migrate.