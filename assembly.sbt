assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadedgoogle.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) =>
    xs map (_.toLowerCase) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}