package FileManipulation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class FileCopyWithProgress {
    public static void main (String[] args) throws Exception {
        String localSrc = args[0];
        String destination  = args[1];
        InputStream inputStream = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(destination),conf);
        OutputStream outputStream = fs.create(new Path(destination), new Progressable() {
            @Override
            public void progress() {
                System.out.println('.');
            }
        });
        IOUtils.copyBytes(inputStream,outputStream,4096,true);

    }


}
