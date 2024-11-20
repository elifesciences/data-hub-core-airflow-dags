elifePipeline {
    node('containers-jenkins-plugin') {
        def image_repo = 'elifesciences/data-hub-core-dags'

        def commit
        def commitShort
        def branch
        def timestamp

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
            commitShort = elifeGitRevision().substring(0, 8)
            branch = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()
            timestamp = sh(script: 'date --utc +%Y%m%d.%H%M', returnStdout: true).trim()
        }

        stage 'Build and run tests', {
            lock('data-hub-core-airflow-dags--ci') {
                withDataPipelineCredentialsFromVault{
                    try {
                        timeout(time: 40, unit: 'MINUTES') {
                            sh "make ci-build-and-end2end-test"
                        }
                    } catch (exc) {
                        sh "make ci-end2end-test-logs"
                        throw exc
                    } finally {
                        sh "docker-compose ps"
                        sh "docker-compose logs"
                        sh "make ci-clean"
                    }
                } 
            }
        }

        stage 'Build main image', {
            sh "make IMAGE_REPO=${image_repo} IMAGE_TAG=${commit} ci-build-main-image"
        }

        elifeMainlineOnly {
            def dev_image_repo = image_repo + '_unstable'

            stage 'Push image', {
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=${commit} IMAGE_REPO=${dev_image_repo} retag-push-image"
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=${branch}-${commitShort}-${timestamp} IMAGE_REPO=${dev_image_repo} retag-push-image"
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=latest IMAGE_REPO=${dev_image_repo} retag-push-image"
            }
        }

        elifeTagOnly { tagName ->
            def candidateVersion = tagName - "v"

            stage 'Push release image', {
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=latest IMAGE_REPO=${image_repo} retag-push-image"
                sh "make EXISTING_IMAGE_TAG=${commit} EXISTING_IMAGE_REPO=${image_repo} IMAGE_TAG=${candidateVersion} IMAGE_REPO=${image_repo} retag-push-image"
            }
        }
    }
}


def withDataPipelineCredentialsFromVault(doSomething) {
    withDataPipelineGcpCredentials{
        withDataPipelineGmailCredentials{
            withDataPipelineGmailTestCredentials{
                doSomething()
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

def withDataPipelineGmailCredentials(doSomething) {
    try {
        sh 'mkdir -p .secrets'
        sh 'vault.sh kv get -field credentials secret/containers/data-hub/gmail > ./.secrets/gmail_credentials.json'
        doSomething()
    } finally {
        sh 'echo > ./.secrets/gmail_credentials.json'
    }
}

def withDataPipelineGmailTestCredentials(doSomething) {
    try {
        sh 'mkdir -p .secrets'
        sh 'vault.sh kv get -field credentials secret/containers/data-hub/gmail-test > ./.secrets/gmail_end2end_test_credentials.json'
        doSomething()
    } finally {
        sh 'echo > ./.secrets/gmail_end2end_test_credentials.json'
    }
}
