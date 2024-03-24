plugins {
    id("java")
    id("checkstyle")
    id("me.champeau.jmh") version "0.7.0"
    id("application")
    id("org.graalvm.buildtools.native") version "0.9.23"
}

group = "com.flash.data"
version = "1.0-SNAPSHOT"

val generatedSrcDir = buildDir.resolve("generated-src")

sourceSets {
    main {
        java.srcDirs("src/main/java", "build/generated-src")
    }
}
configurations {
    create("codecGeneration")
    getByName("compileOnly") {
        extendsFrom(getByName("annotationProcessor"))
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation(libs.fastutil)
    implementation(libs.agrona)

    implementation(libs.picocli)
    implementation(libs.jline)
    implementation(libs.picoJline)

    implementation(libs.bundles.log4j2)

    implementation(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation("org.apache.httpcomponents:httpclient:4.5.14")
    implementation("com.jsoniter:jsoniter:0.9.23")
    implementation("commons-io:commons-io:2.15.1")
    implementation("com.google.guava:guava:32.1.3-jre")
    implementation("com.github.luben:zstd-jni:1.5.5-11")
    implementation("org.agrona:agrona:1.4.0")
    implementation("com.cronutils:cron-utils:9.2.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.mockito)
}

graalvmNative {
        binaries {
            named("main") {
                imageName.set("marketData")
                //mainClass.set("com.flash.feature.impl.FeatureServerCli")
                //buildArgs.add("-H:ReflectionConfigurationFiles=native-image/reflect-config.json")
                // buildType.set(org.graalvm.buildtools.gradle.dsl.NativeImageOptions.BuildType.EXECUTABLE)
                // Additional configurations like buildArgs, runtimeArgs, etc., can be specified here.
                // buildArgs.add("-O4")
            }
            named("test") {
              // buildArgs.add("-O0")
            }
        }
        binaries.all {
            buildArgs.add("--verbose")
        }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    testLogging {
//        events("passed", "skipped", "failed")
//        showStandardStreams = true
    }
}

tasks.named<Jar>("jar") {
    manifest {
        attributes["Main-Class"] = "com.flash.data.tardis.TardisDownloader"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}

jmh {
    benchmarkMode.set(listOf("thrpt"))
    profilers.set(listOf("gc"))
    warmupIterations.set(2)
    iterations.set(2)
    fork.set(1)
    jmhTimeout.set("60s")
    timeUnit.set("ms")
    failOnError.set(true)
    jvmArgs.set(listOf("--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"))
    // Additional configurations such as specific JVM paths can be uncommented and configured similarly
    // jvm.set("/path/to/your/jvm")
}
