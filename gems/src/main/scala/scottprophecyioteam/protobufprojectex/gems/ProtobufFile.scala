package scottprophecyioteam.protobufprojectex.gems

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.datasetSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}


class ProtobufFile extends DatasetSpec {

  val name: String = "ProtobufFile"
  val datasetType: String = "File"

  type PropertiesType = ProtobufFileProperties
  case class ProtobufFileProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Path")
    path: String = "",
    @Property("PROTO")
    PROTO: Option[String] = None,
    @Property("DESCRIPTOR_FILE")
    DESCRIPTOR_FILE: Option[String] = None,
    @Property("AREA")
    AREA: Option[String] = None,
    @Property("NAME")
    NAME_override: Option[String] = None,
    @Property("BASE_YYYYMMDDHH")
    BASE_YYYYMMDDHH: Option[String] = None,
    @Property("HH_OFFSET1")
    HH_OFFSET1: Option[Int] = None,
    @Property("HH_OFFSET2")
    HH_OFFSET2: Option[Int] = None,
    @Property("CHUNK_SIZE")
    CHUNK_SIZE: Option[Int] = None,
    @Property("ALLOW_EMPTY_INPUT")
    ALLOW_EMPTY_INPUT: Option[Boolean] = None
    ) extends DatasetProperties

  implicit val ProtobufFilePropertiesFormat: Format[ProtobufFileProperties] = Json.format


  def sourceDialog: DatasetDialog = {
    val pbufProperties = ScrollBox().addElement(
      StackLayout()
        .addElement(
          StackItem().addElement(TextBox("PROTO").bindPlaceholder("").bindProperty("PROTO"))
        )
        .addElement(
          StackItem().addElement(TextBox("HDFS_PREFIX").bindPlaceholder("").bindProperty("path"))
        )
        .addElement(
          StackItem().addElement(TextBox("DESCRIPTOR_FILE").bindPlaceholder("").bindProperty("DESCRIPTOR_FILE"))
        )
        .addElement(
          StackItem().addElement(TextBox("AREA").bindPlaceholder("").bindProperty("AREA"))
        )
        .addElement(
          StackItem().addElement(TextBox("NAME (override)").bindPlaceholder("PROTO_pb").bindProperty("NAME_override"))
        )
        .addElement(
          StackItem().addElement(TextBox("BASE_YYYYMMDDHH").bindPlaceholder("").bindProperty("BASE_YYYYMMDDHH"))
        )
        .addElement(
          StackItem().addElement(ColumnsLayout(gap = Some("1rem"), height=Some("100%"))
            .addColumn(TextBox("HH_OFFSET1").bindPlaceholder("").bindProperty("HH_OFFSET1"))
            .addColumn(TextBox("HH_OFFSET2").bindPlaceholder("").bindProperty("HH_OFFSET2")))
        )
        .addElement(
          StackItem().addElement(ColumnsLayout(gap = Some("1rem"), height=Some("100%"))
            .addColumn(TextBox("Chunk size").bindPlaceholder("").bindProperty("CHUNK_SIZE"))
            .addColumn(Checkbox("Allow empty input").bindProperty("ALLOW_EMPTY_INPUT")))
        )
    )
    val diag = DatasetDialog("ProtobufFile")
    //.addSection("LOCATION", TargetLocation("path"))
    .addSection("PROTOBUF", pbufProperties)
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
              StackLayout()
                .addElement(
                  StackItem(grow = Some(1)).addElement(
                    FieldPicker(height = Some("100%"))
                  )
                )
            ),
          "auto"
        )
        .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
    )
    .addSection(
      "PREVIEW",
      PreviewTable("").bindProperty("schema")
    )
    diag
  }

  def targetDialog: DatasetDialog = DatasetDialog("ProtobufFile")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%")).addElement(
              StackItem(grow = Some(1)).addElement(
                FieldPicker(height = Some("100%"))
              )
            )
          ),
          "auto"
        )
        .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
    )

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = Nil

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class ProtobufFileFormatCode(props: ProtobufFileProperties) extends ComponentCode {
    def sourceApply(spark: SparkSession): DataFrame = {
      import org.apache.spark.sql.protobuf.functions._
      import org.apache.hadoop.io.{BytesWritable, NullWritable}
      import spark.sqlContext.implicits._

      val descriptorDF = spark.read.format("binaryFile").load(props.DESCRIPTOR_FILE.get)
      val descriptorByteArray = descriptorDF.select("content").collect()(0)(0).asInstanceOf[Array[Byte]]

      def getName() = {
        props.NAME_override.getOrElse(s"${props.PROTO.get}_pb")
      }

      val input_df = spark.sparkContext.sequenceFile(s"${props.path}/${props.AREA.get}/${getName()}/${props.BASE_YYYYMMDDHH.get}", classOf[NullWritable], classOf[BytesWritable]).map {
        case (k, v) => v.copyBytes
      }.toDF

      input_df.select(from_protobuf($"value", props.PROTO.get, descriptorByteArray) as "pbuf_data").select("pbuf_data.*")
      //val path = s"${props.path}/${props.PROTO}/${props.AREA}/${props.NAME_override}/${props.BASE_YYYYMMDDHH}/${props.HH_OFFSET1}/${props.HH_OFFSET2}/${props.CHUNK_SIZE}/${props.ALLOW_EMPTY_INPUT}"
      //var reader = spark.read.format("ProtobufFile")
      //reader.load(path)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write.format("ProtobufFile")
      writer.save(props.path)
    }
  }
  def deserializeProperty(props: String): ProtobufFileProperties = Json.parse(props).as[ProtobufFileProperties]

  def serializeProperty(props: ProtobufFileProperties): String = Json.toJson(props).toString()

}
