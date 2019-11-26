elifePipeline {
    node('containers-jenkins-plugin') {

        stage 'Checkout', {
            checkout scm
        }

        stage 'Build and run tests', {
            withDataPipelineGcpCredentials {
                try {
                    sh "make build-dev"
                    sh "make ci-end2end-test"
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
