# Aqueduct

Delivers data to specific network locations in an efficient, secure and near real-time manner.

Note this is currently a work in progress product, and we wouldn't recommend using it in production yet. We intend to open source other related components and documentation in the near future.

## Objectives

1. Reduce network traffic usage by delivering data to location groups only once in most cases
2. Optimise the time taken to deliver data to a location
3. Throttle traffic not to impact the network
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
 test
 
