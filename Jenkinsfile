@Library("magic_pipe_jenkins_shared_library") _

properties([
        buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '30')),
        disableConcurrentBuilds(),
        disableResume(),
        parameters([booleanParam(name: 'INITIAL_BUILD', defaultValue: false, description: 'Is this the initial build of this project?')])
])

ansiColor('xterm') {

    node('jenkins-deployer') {
        def scmVars = []
        String acrLoginToken = ""
        String registry = "${env.CONTAINER_REGISTRY}.azurecr.io"

        stage("Git") {
            scmVars = checkout scm
        }

        stage("Authenticate") {
            acrLoginToken = authWithPodIdentity([registry: env.CONTAINER_REGISTRY, environment: env.ENVIRONMENT,
                                                 tag: env.TAG, project: env.PROJECT, location: env.location])
        }

        stage("Gradle Build") {
            if(scmVars.GIT_BRANCH == "master") {
                sh "./gradlew createRelease -Prelease.disableChecks"
            }
            sh "./gradlew assemble"
        }

        String integrationImage = "$registry/aqueduct-pipe:integration-${scmVars.GIT_COMMIT}"
        String ppeImage = "$registry/aqueduct-pipe:ppe-${scmVars.GIT_COMMIT}"
        String liveImage = "$registry/aqueduct-pipe:live-${scmVars.GIT_COMMIT}"
        String ppeRollbackImage = "$registry/aqueduct-pipe:ppe-rollback-from-${scmVars.GIT_COMMIT}"
        String latestImage = "$registry/aqueduct-pipe:latest"

        stage('Docker build and Scan') {
            container('docker') {
                sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"
                dir("pipe-http-server-cloud") {
                    sh "docker build -t ${integrationImage} ."
                } 

                sh "docker run --rm  -v /var/run/docker.sock:/var/run/docker.sock -v $HOME/.cache/trivy:/root/.cache/trivy knqyf263/trivy:0.1.2 --quiet --ignore-unfixed $integrationImage"
                sh "docker push ${integrationImage}"

                if (params.INITIAL_BUILD) {
                    sh "docker tag ${integrationImage} ${latestImage}"
                    sh "docker push ${latestImage}"
                }
            }
        }

        stage ('Isolated System test') {
            isolatedSystemTest(MP_AQUEDUCT_PIPE_IMAGE_VERSION: "integration-${scmVars.GIT_COMMIT.toString()}")
        }

        def version = readFile(file:"VERSION.txt")

        if (scmVars.GIT_BRANCH == "master") {
            container('docker') {
                sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"

                stage('Deploy To PPE') {
                    sh "docker tag ${integrationImage} ${ppeImage}"
                    sh "docker push ${ppeImage}"
                }
            }

            stage('PPE Version Test') {
                versionTest("https://api-ppe.tesco.com/messaging/v1/pipe/_status", version)
            }

            stage("PPE Runscope Tests") {
                parallel(
                    get_pipe: {
                        completeRunscopeTests("ppe", "get_pipe")
                    },
                    publisher: {
                        completeRunscopeTests("ppe", "publisher")
                    },
                    registry_v2: {
                        completeRunscopeTests("ppe", "registry_v2")
                    },
                    auth_check: {
                        completeRunscopeTests("ppe", "auth_check")
                    }
                )
            }

            stage("Release") {
                sshagent(credentials: ['public_github_key']) {
                    sh "#!/bin/sh -e\n./gradlew release -Prelease.disableChecks -Prelease.pushTagsOnly"
                }
            }

            stage("Publish") {
                withCredentials([usernamePassword(credentialsId: 'nexus', passwordVariable: 'NEXUS_PASSWORD', usernameVariable: 'NEXUS_USERNAME')]) {
                    sh "#!/bin/sh -e\n./gradlew publish -PmavenUser=$NEXUS_USERNAME -PmavenPassword='$NEXUS_PASSWORD'"
                }
            }

            stage('Deploy to Live') {
                container('docker') {
                    sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"
                    sh "docker tag ${ppeImage} ${liveImage}"
                    sh "docker push ${liveImage}"
                }
            }

            stage('Live Version Test') {
                versionTest("https://api.tesco.com/messaging/v1/pipe/_status", version)
            }


            stage("Live Runscope Tests") {
                parallel(
                    get_pipe: {
                        completeRunscopeTests("live", "get_pipe")
                    },
                    publisher: {
                        completeRunscopeTests("live", "publisher")
                    },
                    registry_v2: {
                        completeRunscopeTests("live", "registry_v2")
                    },
                    auth_check: {
                        completeRunscopeTests("live", "auth_check")
                    }
                )
            }

            container('docker') {
                sh "#!/bin/sh -e\ndocker login $registry -u 00000000-0000-0000-0000-000000000000 -p $acrLoginToken"

                stage('Tag Latest Image') {
                    sh "docker tag ${liveImage} ${latestImage}"
                    sh "docker push ${latestImage}"
                }
            }

        } else {
            stage("GitHub Status") {
                publicGithubStatusUpdate("aqueduct-core", "success", scmVars.GIT_COMMIT)
            }
        }
    }
}
