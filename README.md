Simple example of reading data from mongo database and storing it into Postgres. 


While importing projects and project_users relation, migration ignores non-existing users
assigned at the project, duplicate users, project without assigned users. Migration adds
"migrated_from" field in project metadata to mark that project was imported from MongoDB.