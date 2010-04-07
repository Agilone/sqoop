/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.sqoop.shims;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;

/**
 * Provides a service locator for the appropriate shim, dynamically chosen
 * based on the Hadoop version in the classpath. 
 */
public abstract class ShimLoader {
  private static HadoopShim hadoopShim;

  public static final Log LOG = LogFactory.getLog(ShimLoader.class.getName());

  /**
   * Which directory Sqoop checks for shim jars.
   */
  public static final String SHIM_JAR_DIR_PROPERTY = "sqoop.shim.jar.dir";

  /**
   * The names of the classes for shimming Hadoop.
   * This list must be maintained in the same order as HADOOP_SHIM_MATCHES
   */
  private static final List<String> HADOOP_SHIM_CLASSES =
      new ArrayList<String>();

  /**
   * Patterns to match to identify which shim jar to load when shimming
   * Hadoop.
   * This list must be maintained in the same order as HADOOP_SHIM_MATCHES
   */
  private static final List<String> HADOOP_SHIM_JARS =
      new ArrayList<String>();

  /**
   * The regular expressions compared against the Hadoop version string
   * when determining which shim class to load.
   */
  private static final List<String> HADOOP_SHIM_MATCHES =
      new ArrayList<String>();

  static {
    // These regular expressions will be evaluated in order until one matches.

    // Check 
    HADOOP_SHIM_MATCHES.add("0.20.2-[cC][dD][hH]3.*");
    HADOOP_SHIM_CLASSES.add("org.apache.hadoop.sqoop.shims.CDH3Shim");
    HADOOP_SHIM_JARS.add("sqoop-.*-cloudera.jar");

    // Apache 0.22 trunk
    HADOOP_SHIM_MATCHES.add("0.22-.*");
    HADOOP_SHIM_CLASSES.add("org.apache.hadoop.sqoop.shims.Apache22HadoopShim");
    HADOOP_SHIM_JARS.add("sqoop-.*-apache.jar");

    // Apache 0.22 trunk snapshots often compile with "Unknown" version,
    // so we default to guessing Apache in this case.
    HADOOP_SHIM_MATCHES.add("Unknown");
    HADOOP_SHIM_CLASSES.add("org.apache.hadoop.sqoop.shims.Apache22HadoopShim");
    HADOOP_SHIM_JARS.add("sqoop-.*-apache.jar");
  }

  /**
   * Factory method to get an instance of HadoopShim based on the
   * version of Hadoop on the classpath.
   * @param conf an optional Configuration whose internal ClassLoader 
   * should be updated with the jar containing the HadoopShim.
   */
  public static synchronized HadoopShim getHadoopShim(Configuration conf) {
    if (hadoopShim == null) {
      hadoopShim = loadShim(HADOOP_SHIM_MATCHES, HADOOP_SHIM_CLASSES,
          HADOOP_SHIM_JARS, HadoopShim.class, conf);
    }
    return hadoopShim;
  }

  /**
   * Factory method to get an instance of HadoopShim based on the
   * version of Hadoop on the classpath.
   */
  public static synchronized HadoopShim getHadoopShim() {
    return getHadoopShim(null);
  }

  @SuppressWarnings("unchecked")
  /**
   * Actually load the shim for the current Hadoop version.
   * @param matchExprs a list of regexes against which the current Hadoop
   * version is compared. The first one to hit defines which class/jar to
   * use.
   * @param classNames a list in the same order as matchExprs. This defines
   * what class name to load as the shim class if the Hadoop version matches
   * matchExprs[i].
   * @param jarPatterns a list in the same order as matchExprs. This defines
   * a pattern to select a jar file from which the shim classes should be
   * loaded.
   * @param xface the shim interface that the shim class must match.
   * @param conf an optional Configuration whose context classloader should
   * be updated to the current Thread's contextClassLoader after pushing a
   * new ClassLoader on the stack to load this shim jar.
   */
  private static <T> T loadShim(List<String> matchExprs,
      List<String> classNames, List<String> jarPatterns, Class<T> xface,
      Configuration conf) {
    String version = VersionInfo.getVersion();

    LOG.debug("Loading shims for class : " + xface.getName());
    LOG.debug("Hadoop version: " + version);

    for (int i = 0; i < matchExprs.size(); i++) {
      if (version.matches(matchExprs.get(i))) {
        String className = classNames.get(i);
        String jarPattern = jarPatterns.get(i);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Version matched regular expression: " + matchExprs.get(i));
          LOG.debug("Searching for jar matching: " + jarPattern);
          LOG.debug("Trying to load class: " + className);
        }

        try {
          loadMatchingShimJar(jarPattern, className);
          ClassLoader cl = Thread.currentThread().getContextClassLoader();
          Class clazz = Class.forName(className, true, cl);
          T shim = xface.cast(clazz.newInstance());

          if (null != conf) {
            // Set the context classloader for the base Configuration to
            // the current one, so we can load more classes from the shim jar.
            conf.setClassLoader(cl);
          }

          return shim;
        } catch (Exception e) {
          throw new RuntimeException("Could not load shim in class " +
              className, e);
        }
      }
    }

    throw new RuntimeException("Could not find appropriate Hadoop shim for "
        + version);
  }

  /**
   * Look through the shim directory for a jar matching 'jarPattern'
   * and classload it.
   * @param jarPattern a regular expression which the shim jar's filename
   * must match.
   * @param className a class to classload from the jar.
   */
  private static void loadMatchingShimJar(String jarPattern, String className)
      throws IOException {
    String jarFilename;

    String shimDirName = System.getProperty(SHIM_JAR_DIR_PROPERTY, ".");
    File shimDir = new File(shimDirName);
    if (!shimDir.exists()) {
      throw new IOException("No such shim directory: " + shimDirName);
    }

    String [] candidates = shimDir.list();
    if (null == candidates) {
      throw new IOException("Could not list shim directory: " + shimDirName);
    }

    for (String candidate : candidates) {
      if (candidate.matches(jarPattern)) {
        LOG.debug("Found jar matching pattern " + jarPattern + ": "
            + candidate);
        File jarFile = new File(shimDir, candidate);
        String jarFileName = jarFile.toString();
        ClassLoaderStack.addJarFile(jarFileName, className);
        LOG.debug("Successfully pushed classloader for jar: " + jarFileName);
        return;
      }
    }

    throw new IOException("Could not load shim jar for pattern: "
        + jarPattern);
  }

  private ShimLoader() {
    // prevent instantiation
  }

  /**
   * Given the name of a class, try to load the shim jars and return the Class
   * object referring to that class.
   * @param className a class to load out of the shim jar
   * @return the class object loaded from the shim jar for the given class.
   */
  public static <T> Class<? extends T> getShimClass(String className)
      throws ClassNotFoundException {
    getHadoopShim(); // Make sure shims are loaded.
    return (Class<? extends T>) Class.forName(className,
        true, Thread.currentThread().getContextClassLoader());
  }
}
