elifePipeline {
    node('containers-jenkins-plugin') {
        def commit
        def jenkins_image_building_ci_pipeline = 'process/process-data-hub-airflow-image-update-repo-list'
        def git_url

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
            git_url = getGitUrl()
        }

        stage 'Build and run tests', {
            lock('data-hub-core-airflow-dags--ci') {
                withDataPipelineCredentialsFromVault{
                    try {
                        timeout(time: 40, unit: 'MINUTES') {
                            sh "make ci-build-and-end2end-test"
                        }
                    } finally {
                        sh "docker-compose ps"
                        sh "docker-compose logs"
                        sh "make ci-clean"
                    }
                } 
            }
        }

        elifeMainlineOnly {
            stage 'Build data pipeline image with latest commit', {
                triggerImageBuild(jenkins_image_building_ci_pipeline, git_url, commit)
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

def triggerImageBuild(jenkins_image_building_ci_pipeline, gitUrl, gitCommitRef){
    build job: jenkins_image_building_ci_pipeline,  wait: false, parameters: [string(name: 'gitUrl', value: gitUrl), string(name: 'gitCommitRef', value: gitCommitRef)]
}

def getGitUrl() {
    return sh(script: "git config --get remote.origin.url", returnStdout: true).trim()
}
