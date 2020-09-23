# Aqueduct
[![Total alerts](https://img.shields.io/lgtm/alerts/g/Tesco/aqueduct-core.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/Tesco/aqueduct-core/alerts/)

Delivers data to specific network locations in an efficient, secure and near real-time manner.

## Objectives

1. Reduce network traffic usage, by delivering data to a location only once
2. Optimise the time taken to deliver data to a location
3. Throttle traffic not to impact the overall and local network
4. Only data required by the location is routed to it

## Development Environment

### Prerequisites
1. Java 1.8+
2. Groovy 2.5.4+
3. Gradle 5.4.1+

### Setting up IntelliJ
1. Clone the project from git into a local folder (eg /code/aqueduct-pipe)
2. File -> New -> Project from Existing Sources
3. Select your folder (eg /code/aqueduct-pipe)
4. Import project from external model -> Gradle -> Next
5. &lt;Leave Default Settings&gt; -> Finish
6. Preferences -> Build, Execution, Deployment -> Build Tools -> Gradle -> Runner
   1. Delegate IDE build/run actions to Gradle: &lt;Checked&gt;
   2. Run tests using: Gradle Test Runner
 
 ## Tests
 _./gradlew test integration_
 
## Known Issues

###Issue: 
When running tests, you receive the following error:
```
java.lang.IllegalStateException: Process [/var/folders/my/02z_ghk12h54_k9lrm8vhq8m0000gn/T/embedded-pg/PG-d6f2320465793366f452337868c0d06f/bin/initdb, -A, trust, -U, postgres, -D, /var/folders/my/02z_ghk12h54_k9lrm8vhq8m0000gn/T/epg2029673426718816821, -E, UTF-8] failed
```
######Solution:

Delete the sub-folder of embedded-pg. 
```
rm -rf /var/folders/my/02z_ghk12h54_k9lrm8vhq8m0000gn/T/embedded-pg/PG-d6f2320465793366f452337868c0d06f
```