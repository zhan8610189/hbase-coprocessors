package org.apache.hadoop.hbase.coprocessor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SimpleAccessController extends BaseMasterObserver {
  private List<String> whiteTableList = Lists.newArrayList();
  private static byte[] TEMP_PREX = "temp_".getBytes();
  private static byte[] TEST_PREX = "test_".getBytes();
  private Set<String> adminSet = Sets.newHashSet();
  private static final Log LOG = LogFactory.getLog(SimpleAccessController.class);
  private static final long ALLOC_RELOAD_WAIT = 60 * 1000;
  private long lastSuccessfulReload = 0;

  public SimpleAccessController() {
  }

  private void reloadAdminsIfNecessary() {
    Configuration conf = HBaseConfiguration.create();
    Object adminFile = conf.get("hbase.admin.file");
    if (adminFile == null) {
      ClassLoader cL = Thread.currentThread().getContextClassLoader();
      adminFile = cL.getResource("hbase-admin.xml");
    }

    long time = System.currentTimeMillis();
    long lastModified = 0;
    if (adminFile != null) {
      try {
        if (adminFile instanceof String) {
          File f = new File((String) adminFile);
          lastModified = f.lastModified();
        } else if ((URL) adminFile instanceof URL) {
          URLConnection conn = ((URL) adminFile).openConnection();
          lastModified = conn.getLastModified();
        }
        if (lastModified > lastSuccessfulReload && time > lastModified + ALLOC_RELOAD_WAIT) {
          LOG.info("reload the hbase-admin.xml file for Renren hbase security");
          if (adminFile instanceof String) {
            conf.addResource((String) adminFile);
          } else {
            conf.addResource((URL) adminFile);
          }
          lastSuccessfulReload = time;
          whiteTableList.clear();
          whiteTableList.addAll(conf.getStringCollection("hbase.admin.white.tables"));
          adminSet.clear();
          adminSet.addAll(conf.getStringCollection("hbase.admin.users"));
        }
      } catch (IOException e) {
        LOG.warn("Load the hbase-admin.xml file failed ", e);
      }
    }
  }

  /**
   * Returns the active user to which authorization checks should be applied. If
   * we are in the context of an RPC call, the remote user is used, otherwise
   * the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = User.getCurrent();
    }

    return user;
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
      HRegionInfo[] regions) throws IOException {
    byte[] tableName = desc.getName();
    if (Bytes.startsWith(tableName, TEMP_PREX) || Bytes.startsWith(tableName, TEST_PREX) || desc.isMetaTable()) {
      return;
    }
    reloadAdminsIfNecessary();
    User user = getActiveUser();
    checkAdmin(user);
    LOG.info("Admin User " + user.getShortName() + " is creating table " + new String(tableName));
  }

  private void checkAdmin(User user) throws IOException {
    if (user == null || !adminSet.contains(user.getShortName())) {
      String msg = "User " + user.getShortName()
          + " is not the Administrator. non-administrator cannot operate(include create/delete/modify/truncate) table,"
          + " but can operate the table which is prefixed with \"temp_\" or \"test_\","
          + " please contact the HBase administrator(renren.dap@renren-inc.com).";
      LOG.warn(msg);
      throw new IOException(msg);
    }
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
    if (Bytes.startsWith(tableName, TEMP_PREX) || Bytes.startsWith(tableName, TEST_PREX)) {
      return;
    }
    reloadAdminsIfNecessary();
    checkWhiteTable(new String(tableName));
    User user = getActiveUser();
    checkAdmin(user);
    LOG.info("Admin User " + user.getShortName() + " is deleting table " + new String(tableName));
  }

  private void checkWhiteTable(String tableName) throws IOException {
    if (whiteTableList.contains(tableName) || isPrefixWhiteTable(tableName)) {
      String msg = "Table " + tableName
          + " can not deleted. if you want to, please contact the HBase administrator(renren.dap@renren-inc.com).";
      LOG.warn(msg);
      throw new IOException(msg);
    }
  }

  private boolean isPrefixWhiteTable(String tableName) {
    for (String prefix : whiteTableList) {
      if (prefix.charAt(prefix.length() - 1) == '*') {
        prefix = prefix.substring(0, prefix.length() - 1);
      }
      if (tableName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HTableDescriptor htd)
      throws IOException {
    if (Bytes.startsWith(tableName, TEMP_PREX) || Bytes.startsWith(tableName, TEST_PREX)) {
      return;
    }
    reloadAdminsIfNecessary();
    User user = User.getCurrent();
    checkAdmin(user);
    LOG.info("Admin User " + user.getShortName() + " is modifying table " + new String(tableName));
  }
}
