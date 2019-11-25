elifePipeline {
    node('containers-jenkins-plugin') {
        def commit

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
        }

        stage 'Build and run tests', {
        withDataPipelineGcpCredentials {
                try {
                    sh "make build-dev"
                    sh "make build-dev"
                } finally {
                    sh "make ci-clean"
                }
            }
        }
    }
}

def withDataPipelineGcpCredentials(doSomething) {
    try {
        sh 'vault.sh kv get -field credentials secret/containers/data-pipeline/gcp > credentials.json'
        doSomething()
    } finally {
        sh 'echo > credentials.json'
    }
}