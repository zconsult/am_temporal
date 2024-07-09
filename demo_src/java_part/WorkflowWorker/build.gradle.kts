plugins {
        java
        application
}

version = "0.0.2"

dependencies {
        implementation(project(":Impl"))
 
        implementation(group="io.temporal", name = "temporal-sdk", version = "1.22.2")
        implementation(group="org.slf4j", name = "slf4j-nop", version = "2.0.6")
        
        
}

application {
        mainClass.set("com.alfie.temporal.apps.BacktesterWorkflowWorker")
}

tasks.named<JavaExec>("run") {
        standardInput = System.`in`
}
