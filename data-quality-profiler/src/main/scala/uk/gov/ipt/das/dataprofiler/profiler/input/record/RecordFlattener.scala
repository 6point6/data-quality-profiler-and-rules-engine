package uk.gov.ipt.das.dataprofiler.profiler.input.record

import uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor.KeyPreProcessor
import uk.gov.ipt.das.dataprofiler.profiler.input.record.notation.Notation
import uk.gov.ipt.das.dataprofiler.value.{ARRAY, BOOLEAN, DOUBLE, FLOAT, INT, LONG, NULL, NullValue, RECORD, RecordValue, STRING}

case class RecordFlattener private (keyPreProcessor: KeyPreProcessor, notation: Notation) {

  def flatten(record: ProfilableRecord): FlattenedProfilableRecord =
    FlattenedProfilableRecord(
      id = record.getId.getOrElse("_UNKNOWN"),
      flatValues = parseBranch(flatPath = "", fullyQualifiedPath = "", record = record, notation),
      additionalIdentifiers = record.additionalIdentifiers
    )

  private def parseValue(flatPath: String, fullyQualifiedPath: String, v: RecordValue, notation: Notation): Seq[FlatValue] =
    v.valueType match {
      case NULL | STRING | BOOLEAN | INT | LONG | FLOAT | DOUBLE => List(FlatValue(flatPath, fullyQualifiedPath, v))
      case ARRAY if (v.asArray.nonEmpty) => v.asArray.zipWithIndex.flatMap {
      case (arrV: RecordValue, index: Int) => parseValue(s"$flatPath[]", createFullyQualifiedPath(fullyQualifiedPath, index, notation), arrV, notation)
      }
      case ARRAY if (v.asArray.isEmpty) => List(FlatValue(s"$flatPath[]", fullyQualifiedPath, NullValue()))
      case RECORD => parseBranch(flatPath, fullyQualifiedPath, v.asRecord, notation)
    }

  private def createFullyQualifiedPath(fullyQualifiedPath: String, index: Int, notation: Notation): String = {
    notation match {
      case Notation.DotNotation => s"$fullyQualifiedPath${notation.leftNotation}$index${notation.rightNotation}"
      case Notation.SquareBracketsNotation => s"$fullyQualifiedPath${notation.leftNotation}$index${notation.rightNotation}"
    }
  }

  private def stringFilter(key: String): String =
    keyPreProcessor.keyPreProcessor(key)

  private def parseBranch(flatPath: String, fullyQualifiedPath: String, record: ProfilableRecord, notation: Notation): Seq[FlatValue] = {
    def genPath(path: String, key: String): String =
      if (path == "") stringFilter(key) else s"$path.${stringFilter(key)}"

    record.getEntries.flatMap { case (key: String, value: RecordValue) =>
      parseValue(genPath(flatPath, key), genPath(fullyQualifiedPath, key), value, notation)
    }
  }

}
object RecordFlattener {
  def apply(keyPreProcessor: KeyPreProcessor, notation: Notation): RecordFlattener =
    new RecordFlattener(keyPreProcessor, notation)
}