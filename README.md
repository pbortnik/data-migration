Simple example of reading logs from ReportPortal database.
Uses embedded H2 database as a JobRepository.
One job that contains one step. Step contains ItemReader that is presented as MongoItemReader, ItemProcessor that does nothing 
and ItemWriter that just shows data.