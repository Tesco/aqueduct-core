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

        stage('Spot Bugs') {
            sh "./gradlew spotbugsMain"
            def spotbugs = scanForIssues tool: spotBugs(pattern: '**/spotbugs/main.xml')
            publishIssues issues: [spotbugs]
        }

        stage('Pmd Analysis') {
            sh "./gradlew pmdMain"
            def pmd = scanForIssues tool: pmdParser(pattern: '**/pmd/main.xml')
            publishIssues issues: [pmd]
        }

        stage('OWASP Scan') {
            sh "./gradlew dependencyCheckAggregate"
            publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, keepAll: false, reportDir: 'build/reports', reportFiles: 'dependency-check-report.html', reportName: 'Dependency Check', reportTitles: 'Dependency Check Report'])
        }

        stage('Unit Test') {
            try {
                sh "./gradlew test"
            } catch (err) {
                junit '**/build/test-results/test/*.xml'
                throw err
            }
        }

        stage('Integration Test') {
            try {
                sh "./gradlew integration"
            } catch (err) {
                junit '**/build/test-results/test/*.xml'
                throw err
            }
        }

        stage('Publish Test Report') {
            junit '**/build/test-results/test/*.xml'
        }

        stage ('Publish Sonar') {
            try {
                sonarReport("aqueduct_core", scmVars.GIT_BRANCH)
            } catch (err) {
                echo "Error publishing Sonar. Continuing."
                echo "$err"
            }
        }

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
                        checkRunscopeTests("https://api.runscope.com/radar/a611d773-cf82-4556-af26-68b5ac7469e0/trigger?runscope_environment=c8b9298d-9307-4161-902d-7c6998d0563c")
                    },
                    publisher: {
                        checkRunscopeTests("https://api.runscope.com/radar/b6c232b5-61e3-46e4-bc05-cce4fca3c776/trigger?runscope_environment=33bc19df-7cd7-4f74-b9f2-c77daa59b1cb")
                    },
                    registry_v2: {
                        checkRunscopeTests("https://api.runscope.com/radar/0d1686a7-d1ca-481b-8cc0-88b4fcd61340/trigger?runscope_environment=1ad753b5-67d6-454a-af00-21ffbf54bd0d")
                    },
                    auth_check: {
                        checkRunscopeTests("https://api.runscope.com/radar/24bcd68f-9d3c-412d-bb13-89ec5f1f7dd6/trigger?runscope_environment=0a0e122f-8600-4145-8481-16ebc349654f")
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
                    sh "#!/bin/sh -e\n./gradlew publish -PmavenUser=${NEXUS_USERNAME} -PmavenPassword=${NEXUS_PASSWORD}"
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
                        checkRunscopeTests("https://api.runscope.com/radar/3382d06f-8d8b-4bdf-aeaa-a8ea28d5ee03/trigger?runscope_environment=067344b4-1a23-4302-8ffb-1d52aebb9f5c")
                    },
                    publisher: {
                        checkRunscopeTests("https://api.runscope.com/radar/d51b4186-a47d-4331-87c9-810ad7e0bc4a/trigger?runscope_environment=90bdcabb-c5ff-4af4-8b9e-3016280f7eff")
                    },
                    registry_v2: {
                        checkRunscopeTests("https://api.runscope.com/radar/0fd90e73-dc5f-48df-ab01-49ebf4c801f4/trigger?runscope_environment=27189652-3112-42e6-b5a9-f3c060585756")
                    },
                    auth_check: {
                        checkRunscopeTests("https://api.runscope.com/radar/1a7bd2da-e85d-4887-ad50-070253dd8159/trigger?runscope_environment=88ed6aee-b93b-45bd-92a7-a07ac47c854c")
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
