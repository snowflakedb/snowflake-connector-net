@Library('pipeline-utils')
import com.snowflake.DevEnvUtils
import groovy.json.JsonOutput

timestamps {
  node('regular-memory-node') {
    stage('checkout') {
      scmInfo = checkout scm
      println("${scmInfo}")
      env.GIT_BRANCH = scmInfo.GIT_BRANCH
      env.GIT_COMMIT = scmInfo.GIT_COMMIT
    }

    stage('Authenticate Artifactory') {
      script {
        new DevEnvUtils().withSfCli {
          sh "sf artifact oci auth"
        }
      }
    }

    stage('Build') {
      withCredentials([
        usernamePassword(credentialsId: '063fc85b-62a6-4181-9d72-873b43488411', usernameVariable: 'AWS_ACCESS_KEY_ID', passwordVariable: 'AWS_SECRET_ACCESS_KEY'),
      ]) {
        sh '''\
        |#!/bin/bash -e
        |export GIT_BRANCH=${GIT_BRANCH}
        |export GIT_COMMIT=${GIT_COMMIT}
        |$WORKSPACE/ci/build.sh
        '''.stripMargin()
      }
    }

    def params = [
      string(name: 'svn_revision', value: 'bptp-stable'),
      string(name: 'branch', value: 'main'),
      string(name: 'client_git_commit', value: scmInfo.GIT_COMMIT),
      string(name: 'client_git_branch', value: scmInfo.GIT_BRANCH),
      string(name: 'TARGET_DOCKER_TEST_IMAGE', value: 'dotnet-ubuntu204-net9'),
      string(name: 'parent_job', value: env.JOB_NAME),
      string(name: 'parent_build_number', value: env.BUILD_NUMBER)
    ]

    stage('Test') {
      parallel(
        'Test': {
          stage('Test') {
            build job: 'RT-LanguageDotnet-PC', parameters: params
          }
        },
        'Test Authentication': {
          stage('Test Authentication') {
            withCredentials([
              string(credentialsId: 'sfctest0-parameters-secret', variable: 'PARAMETERS_SECRET')
            ]) {
              sh '''\
              |#!/bin/bash -e
              |$WORKSPACE/ci/test_authentication.sh
              '''.stripMargin()
            }
          }
        },
        'Test WIF': {
          stage('Test WIF') {
            withCredentials([
              string(credentialsId: 'sfctest0-parameters-secret', variable: 'PARAMETERS_SECRET')
            ]) {
              sh '''\
              |#!/bin/bash -e
              |$WORKSPACE/ci/test_wif.sh
              '''.stripMargin()
            }
          }
        }
      )
    }
  }
}

pipeline {
  agent { label 'regular-memory-node' }
  options { timestamps() }
  environment {
    COMMIT_SHA_LONG = sh(returnStdout: true, script: "echo \$(git rev-parse HEAD)").trim()
    BASELINE_BRANCH = "${env.CHANGE_TARGET}"
  }
  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }
  }
}

def wgetUpdateGithub(String state, String folder, String targetUrl, String seconds) {
  def ghURL = "https://api.github.com/repos/snowflakedb/snowflake-connector-net/statuses/$COMMIT_SHA_LONG"
  def data = JsonOutput.toJson([state: "${state}", context: "jenkins/${folder}", target_url: "${targetUrl}"])
  sh "wget ${ghURL} --spider -q --header='Authorization: token $GIT_PASSWORD' --post-data='${data}'"
}