package my.home.mapred;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class StringUtils
{
  static String joinString(InputStream stream, String separator)
  {
    return joinString(new BufferedReader(new InputStreamReader(stream)).lines(), separator);
  }

  private static String joinString(Stream<String> stream, String separator)
  {
    return stream.collect(Collectors.joining(separator));
  }
}
