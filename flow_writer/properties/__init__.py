config = {
    'project-meta': 'default',
    'codebuild': {
        'build-name': 'csip53-data-flow-build'
    },
    'logs': {
        'pipeline': {
            'local': 'local: logs/pipeline.log',
            's3-bucket-name': 'pipeline-logs'
        }
    }
}

config_test = {

}