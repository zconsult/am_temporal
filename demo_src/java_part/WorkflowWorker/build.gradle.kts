plugins {
        java
        application
}

version = "0.0.3"

dependencies {
        implementation(project(":Impl"))
 
        implementation(group="io.temporal", name = "temporal-sdk", version = "1.22.2")
        implementation(group="org.slf4j", name = "slf4j-nop", version = "2.0.6")
        implementation(group = "commons-cli", name = "commons-cli", version = "1.8.0")
}

application {
        mainClass.set("com.alfie.temporal.apps.BacktesterWorkflowWorker")
}

tasks.withType<JavaCompile>{
        options.release.set(17)
}

tasks.named<JavaExec>("run") {
        standardInput = System.`in`
}
