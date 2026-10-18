import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.hadoop.fs.*;
public class Probe {
 public static void main(String[] args) throws Exception {
  try(var stream=HadoopInputFile.fromLocation(args[0],new org.apache.hadoop.conf.Configuration()).newStream()) {
   System.out.println("Iceberg stream: "+stream.getClass().getName());
   var delegate=((DelegatingInputStream)stream).getDelegate();
   System.out.println("Delegate: "+delegate.getClass().getName());
   var fs=(FSDataInputStream)delegate;
   System.out.println("Wrapped: "+fs.getWrappedStream().getClass().getName());
   System.out.println("Wrapped ByteBufferReadable: "+(fs.getWrappedStream() instanceof ByteBufferReadable));
   System.out.println("readbytebuffer capability: "+fs.hasCapability(StreamCapabilities.READBYTEBUFFER));
  }
 }
}
