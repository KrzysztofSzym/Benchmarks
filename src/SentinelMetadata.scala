//TODO argumenty 
//1 centre_gdansk
//2 centre_gdansk_32634
//3 dane o układzie współrzędnych np 32634
package src.main.scala

import scala.xml.XML
import java.awt.geom.{Path2D, Rectangle2D, Area}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SentinelMetadata{
  def main(args: Array[String]){
    val startTime=System.currentTimeMillis
    //wersja bazy danych jeżeli 0 to mała baza jeżeli 1 to duża
    val base_version=args(0).toInt
    //jakie miejsce ma przeszukać
    //0 Gdańsk
    //1 Warszawa
    //2 Berlin
    //3 Amsterdam
    //4 Bruksela
    //5 Rzym
    //6 Paryż
    //7 Madryt
    //8 Budapeszt
    //9 Ateny
    val place=args(1).toInt

    val sparkConf = new SparkConf().setAppName("ScalaSentinelMetadata")
    val sc = new SparkContext(sparkConf)

    place match{
      case 0 =>{
        val centre_gdansk:Tuple2[Double, Double]=(54.352123, 18.648036)
        //TODO pozycje w róznych układach współrzędnych
        //val centre_gdansk_32633:Tuple2[Double, Double]=(737053.16, 6028836.17)
        val centre_gdansk_32634:Tuple2[Double, Double]=(347147.58, 6025250.17)
        val rectangle_gdansk_wsg84:Rectangle2D.Double=new Rectangle2D.Double(54.3196338, 18.5960678, 0.0649566, 0.1040181)
        //val centre_gdansk_32635:Tuple2[Double, Double]=(-42215.59, 6054910.09)
      }
      case 1 =>
    }

    def bound_rectangle(cloud_pos:String, centre_of_rectangle:Tuple2[Double, Double]) : Boolean ={
      val cloud_positions : Array[Double] = cloud_pos.split(" ").map(_.toDouble);
      val cloud_path : Path2D = new Path2D.Double();
      cloud_path.moveTo(cloud_positions(0),cloud_positions(1))
      for(Array(x,y)<-cloud_positions.grouped(2).drop(1))cloud_path.lineTo(x,y)
      val cloud_area:Area=new Area(cloud_path)
      val target_rectangle=new Rectangle2D.Double(centre_of_rectangle._1-3500, centre_of_rectangle._2-3500, 7000.0, 7000.0)
      !cloud_area.intersects(target_rectangle)
    }

    def bound_rectangle_wsg84(cloud_pos:String, target_rectangle:Rectangle2D.Double) : Boolean ={
      val cloud_positions : Array[Double] = cloud_pos.split(" ").map(_.toDouble);
      val cloud_path : Path2D = new Path2D.Double();
      cloud_path.moveTo(cloud_positions(0),cloud_positions(1))
      for(Array(x,y)<-cloud_positions.grouped(2).drop(1))cloud_path.lineTo(x,y)
      val cloud_area:Area=new Area(cloud_path)
      cloud_area.contains(target_rectangle)
    }

    def filter_bounding_box(positionsString:String,position_test:Tuple2[Double, Double]): Boolean = {
      val positions: Array[Double] = positionsString.split(" ").map(_.toDouble)
      val product_area:Path2D = new Path2D.Double();
      product_area.moveTo(positions(0),positions(1));
      for(Array(lat, lon) <- positions.grouped(2).drop(1))
        product_area.lineTo(lat,lon)
      product_area.contains(position_test._1, position_test._2)
    }
    base_versionmatch match{
      case 0 => var sentinel_global_metadata=sc.wholeTextFiles("swift2d://sentinel_gda.sentinel/sentinel12/../*MSIL[12][AC].xml")
      case 1 => var sentinel_global_metadata=sc.wholeTextFiles("swift2d://products.sentinel/sentinel2/../*MSIL[12][AC].xml")
    }
    //Mała baza
    //var sentinel_global_metadata=sc.wholeTextFiles("swift2d://sentinel_gda.sentinel/sentinel12/../*MSIL[12][AC].xml")

    //Duża baza
    var sentinel_global_metadata=sc.wholeTextFiles("swift2d://products.sentinel/sentinel2/../*MSIL[12][AC].xml")

    var global_xml=sentinel_global_metadata.map{case(name, global)=>(name, XML.loadString(global))}

    var global_details=global_xml.map{case(name, xml)=>(name, xml \\ "EXT_POS_LIST" text, xml \\ "Granule" text)}

    //var global_filter=global_details.filter{case(name, positions_list, granule)=>filter_bounding_box(positions_list, centre_gdansk)}
    var global_filter=global_details.filter{case(name, positions_list, granule)=>bound_rectangle_wsg84(positions_list, rectangle_gdansk_wsg84)}

    //TODO scieżka do pliku bez znaku nowej lini najprawdopodobniej do poprawy część z granule.substring
    var global_and_cloud=global_filter.map{case(name, positions_list, granule)=>
      (name, (name.substring(0, name.length-14)+granule.substring(13,56)+"QI_DATA/MSK_CLOUDS_B00.gml"))}.cache()

    var global_and_cloud2=sc.parallelize(global_and_cloud.collect.map{case(name, path)=>
      (name,
        try{sc.wholeTextFiles(path).first._2}
        catch {
          case e: org.apache.hadoop.mapreduce.lib.input.InvalidInputException => None
          case e: java.lang.StringIndexOutOfBoundsException => None} )})

    var global_and_cloud3=global_and_cloud2.filter{case(name, cloud_content)=>
      !cloud_content.equals(None)}

    var global_and_cloud4=global_and_cloud3.map{case(name, cloud_content)=>
      (name, cloud_content.toString)}

    var global_and_cloud_xml=global_and_cloud4.map{case(name, cloud_content)=>(name, XML.loadString(cloud_content))}

    var global_and_cloud_details=global_and_cloud_xml.map{case(name, cloud_xml)=>(name, (cloud_xml \\ "Envelope" \@ "srsName"),(cloud_xml \\ "Polygon").map(_ \\ "posList" text))}

    var global_and_cloud_details2=global_and_cloud_details.filter{case(name, epsg, position_list)=>
      epsg.endsWith("634")}

    var global_and_cloud_details3=global_and_cloud_details2.map{case(name, epsg, position_list)=>
      (name, {position_list.map{case(item)=>bound_rectangle(item, centre_gdansk_32634)}.toList})}

    val no_cloud_pictures=global_and_cloud_details3.map{case(name, pos)=>(name, !(pos.contains(false)))}

    val result=no_cloud_pictures.filter{case(name, pos)=>pos==true}.count
    //odfiltruj zdjecia bezchmurne na zadanym obszarze
    println("Czas trwania: "+(System.currentTimeMillis-startTime)/1000+"s ilosc zobrazowań spełniających kryteria :" + result)
  }
}