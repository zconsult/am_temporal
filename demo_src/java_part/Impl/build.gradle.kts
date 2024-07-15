plugins {
        `java-library`
}

version = "0.0.1"

dependencies {
        implementation(group="io.temporal", name = "temporal-sdk", version = "1.22.2")
        implementation(group = "commons-cli", name = "commons-cli", version = "1.8.0")
        implementation(group = "org.yaml", name = "snakeyaml", version = "2.2")

}

tasks.withType<JavaCompile>{
options.release.set(17)
}