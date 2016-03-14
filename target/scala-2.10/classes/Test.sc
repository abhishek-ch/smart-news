import scala.collection.mutable.ArrayBuffer

val longVal = ArrayBuffer("12:345:56 78;#inboundmarketing: #Buntha What Does @awesome :Marketing Look Like Around the World? 36 Stats & Trends From 5 Different Regions http://mvl.me/1PYE8BM  #dig…;Choudhary#123;I#123s;Testing #123;It;*)ok^&GH" +
  "ht;http://www.google.come; ok I am enjoying it https://checkitout;" +
  "oh man thats, niot somet's but that - natural")
val names = longVal.map(line => line.split(";").map(_.replaceAll("#inboundmarketing","**")))

for (each <- names){
  for(ea <- each) {
    println(" => "+ea)
    var input: String = ea
    var expr = """http/|https[^\\s]+""".r
    input =  expr.replaceAllIn(input ,"")
     expr = """#(\w+)""".r
    input =  expr.replaceAllIn(input ,"")
    expr = """@(\w+)""".r
    input =  expr.replaceAllIn(input ,"")
    expr = """[^A-Za-z0-9 ]""".r
    input =  expr.replaceAllIn(input ,"")
    //input = input.replaceAll("[^a-zA-Z0-9 ]", "")
    println(" Af8 "+input)
  }
}
println()
var expr = """#abhishek""".r
var input = "Abhishek is #abhishek but yoy just cant #abhishek it"
expr.replaceAllIn(input,"")
input.replaceAll("#abhishek","")
//http://stackoverflow.com/questions/20171305/multi-hashtag-system-in-scala-with-play-framework


var x: Map[String,String] = Map()
x += ("AK" -> "Alaska"); println(x)
val value:String = x.getOrElse("AK","")
x += ("AK" -> (value+" "+"pappu")); println(x)
x += ("NO" -> "Session")
val value1:String = x.getOrElse("NO","")
x += ("NO" -> (value1+" "+"OH! Shit")); println(x)
val value2:String = x.getOrElse("AK","")
x += ("AK" -> (value2+" "+"Lier lier")); println(x)
x.keys
