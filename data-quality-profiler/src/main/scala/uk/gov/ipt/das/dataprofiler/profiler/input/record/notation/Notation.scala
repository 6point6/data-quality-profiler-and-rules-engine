package uk.gov.ipt.das.dataprofiler.profiler.input.record.notation

sealed abstract class Notation(val leftNotation: String, val rightNotation: String) extends Serializable

object Notation {
  final case object DotNotation extends Notation(".", "")
  final case object SquareBracketsNotation extends Notation("[", "]")
}
