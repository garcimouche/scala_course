import sbt._
import Keys._
import ch.epfl.lamp.CourseraBuild
import ch.epfl.lamp.SbtCourseraPlugin.autoImport._

object ParprogBuild extends CourseraBuild {

  override def assignmentSettings: Seq[Setting[_]] = Seq(
    // This setting allows to restrict the source files that are compiled and tested
    // to one specific project. It should be either the empty string, in which case all
    // projects are included, or one of the project names from the projectDetailsMap.
    currentProject := "",

    // TODO:
    // Some assignments need multiple common packages, and rely on lecture code.
    // If we add a field for additional source folders to ProjectDetailsMap,
    // we can avoid this.
    // This would need to be added in the sbt-coursera plugin.
    commonSourcePackages := {
      currentProject.value match {
        // TODO: "lectures/algorithms" doesn't work
        // (see https://github.com/sbt-coursera/sbt-coursera/blob/master/src/main/scala/ch/epfl/lamp/SbtCoursera.scala#L515)
        case "barneshut" => Seq("lectures", "common")
        case _ => Seq("common")
      }
    },

    gradingTestPackages += "grading",

    // Files that we hand out to the students
    handoutFiles <<= (baseDirectory, projectDetailsMap, commonSourcePackages) map {
      (basedir, detailsMap, commonSrcs) =>
      (projectName: String) => {
        val details = detailsMap.getOrElse(projectName, sys.error("Unknown project name: "+ projectName))
        val commonFiles = (PathFinder.empty /: commonSrcs)((files, pkg) =>
            files +++ (basedir / "src" / "main" / "scala" / pkg ** "*.scala")
        )
        (basedir / "src" / "main" / "scala" / details.packageName ** "*.scala") +++
        commonFiles +++
        (basedir / "src" / "main" / "resources" / details.packageName ** "*.*") +++
        (basedir / "src" / "test" / "scala" / details.packageName ** "*.scala") +++
        (basedir / "build.sbt") +++
        (basedir / "project" / "build.properties") +++
        (basedir / "project" ** ("*.scala" || "*.sbt")) +++
        (basedir * (".classpath" || ".project")) +++
        (basedir / ".settings" / "org.scala-ide.sdt.core.prefs")
      }
    })
}
