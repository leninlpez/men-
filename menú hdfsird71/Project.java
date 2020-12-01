package hdfsird71;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class hdfsird71 {

private static String nuevaCarpeta = "Definir el nombre de la carpeta";
private static String nombreArchivo = "World";
private static String contenido = "hellow";

private static String rutaHDFS = "/World";

private static String rutaLocal =  "/home/cloudera/nidea";
private static String archivoLocal = "/home/cloudera/lenin/Local";

public static void main(String[] args) {

//Creamos la configuración de acceso al HDFS
Configuration conf = new Configuration(true);
conf.set("fs.defaultFS", "hdfs://10.0.2.15:8020/");

System.setProperty("HADOOP_USER_NAME", "hdfs");

try {
//Crear objeto FileSystem
FileSystem fs = FileSystem.get(conf);

String home = fs.getHomeDirectory().toString();

//En caso de que no exista la carpeta, crear la carpeta.
if(!fs.exists(new Path(home + "/" + nuevaCarpeta))) {
fs.mkdirs(new Path(home + "/" + nuevaCarpeta));
}

//Si no existe el archivo, hay que crearlo
Path rutaArchivo = new Path(home + "/" + nuevaCarpeta + "/" + nombreArchivo);
FSDataOutputStream outputStream = null;

if(!fs.exists(rutaArchivo)) {
outputStream = fs.create(rutaArchivo);
outputStream.writeBytes(contenido);
outputStream.close();
}

//Vamos a leer el archivo que acabamos de escribir.
Path rutaarchivo = new Path(rutaHDFS + "/" + nombreArchivo);

FSDataInputStream inputStream = fs.open(rutaarchivo);
String salida = IOUtils.toString(inputStream, "UTF-8");
inputStream.close();

System.out.println(salida);

//Podemos ver el estado del archivo.
FileStatus status = fs.getFileStatus(rutaArchivo);

//Tambien podemos modificar el propietario o los permisos del archivo.
fs.setOwner(rutaArchivo, "cloudera", "cloudera");

FsPermission permisos = FsPermission.valueOf("-rwxrwxrwx");
fs.setPermission(rutaArchivo, permisos);

//Al igual que hemos realizado con la línea de comandos, podemos mover archivo de Local al HDFS y viceversa.
//Local --> HDFS
fs.copyFromLocalFile(false, true, new Path(archivoLocal), new Path(rutaHDFS));
//HDFS --> Local
fs.copyToLocalFile(false, rutaArchivo, new Path(rutaLocal));

//Por ultimo, borraremos el directorio y los archivos.
fs.delete(new Path(rutaHDFS), true);
fs.close();

} catch (IOException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}
}
