package com.efun.mainland.util.zookeeper;

import com.efun.mainland.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class ZKCurator {

    private static final Logger logger = LoggerFactory.getLogger(ZKCurator.class);

    private static CuratorFramework zkclient;
    private static TreeCache treeCache;

    private static volatile boolean state = false;

    private static final String ZOOKEEPER_FILE = "zookeeper.properties";

    static {
        String area = PropertyConfigurer.getRegionSystemProperty();
        String path;
        if (area != null) {
            path = PropertiesFileLoader.getClassPath() + area + File.separator;
        } else {
            path = PropertiesFileLoader.getClassPath();
        }
        File file = new File(path, ZOOKEEPER_FILE);
        if (file.exists()) {
            try {
                init();
            } catch (Exception e) {
                logger.error("Exception Message:" + e.getMessage(), e);
            }
        } else {
            logger.info("classpath:zookeeper.properties not exists!");
            System.out.println("classpath:zookeeper.properties not exists!");
        }
    }

    /**
     * 检查zookeeper集群服务是否可用
     *
     * @return
     */
    public static boolean checkState() {
        return state;
    }

    public synchronized static void init() throws Exception {
        if (state) {
            logger.info("inited");
            return;
        }
        state = false;
        String zkhost = PropertiesCacheUtil.getValue("zk.cluster.nodes", ZOOKEEPER_FILE);// zk的host

        if (StringUtils.isBlank(zkhost)) {
            logger.info("zkhost is blank");
            return;
        }

        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);// 重试机制
        Builder builder = CuratorFrameworkFactory.builder().connectString(zkhost).connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000).retryPolicy(rp);

        zkclient = builder.build();
        zkclient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState.isConnected()) {
                    logger.info("connected established:" + newState);
                    System.out.println("connected established:" + newState);
                } else {
                    logger.error("connection lost,waiting for reconection:" + newState);
                    System.out.println("connection lost,waiting for reconection:" + newState);
                    try {
                        logger.info("reinit---");
                        reinit();
                        logger.info("inited---");
                    } catch (Exception e) {
                        logger.error("re-inited failed:" + e.getMessage(), e);
                    }
                }

            }
        });
        zkclient.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                // TODO Auto-generated method stub
                logger.info("eventReceived:" + event);
            }
        });

        zkclient.start();// 放在这前面执行
        logger.info("connecting zookeeper:" + zkhost);
        System.out.println("connecting zookeeper:" + zkhost);
        // 连接成功后，才进行下一步的操作
        state = zkclient.blockUntilConnected(10, TimeUnit.SECONDS);

        final String webroot = PropertiesFileLoader.getWebRoot();
        final String classpath = PropertiesFileLoader.getClassPath();

        final String zkpath = loadZKpath(null);
        logger.info("connected:{}.project zookeeper path:{}", state, zkpath);
        System.out.println("connected:" + state + ".project zookeeper path:" + zkpath);

        InterProcessMutex lock = new InterProcessMutex(zkclient, zkpath);
        if (lock.acquire(5, TimeUnit.SECONDS)) {
            try {
                if (!checkDirectoryExists(zkpath)) {
                    createrOrUpdateNode(zkpath, zkpath);
                }
            } finally {
                lock.release();
            }
        }

        treeCache = new TreeCache(zkclient, zkpath);
        treeCache.start();
        treeCache.getListenable().addListener(new TreeCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                // TODO Auto-generated method stub
                try {

                    ChildData childData = event.getData();
                    logger.info("treeCache type:{},path:{},stat:{}", event.getType(),
                            childData == null ? "null" : childData.getPath(),
                            childData == null ? "null" : childData.getStat());

                    // System.out.println("treeCache:" + event);
                    // System.out
                    // .println(
                    // "treeCache data:"
                    // + (event.getData() != null
                    // ? (event.getData().getData() != null
                    // ? new String(event.getData().getData(), "UTF-8") : "")
                    // : ""));
                    // logger.info("treeCache:{}", event);

                    // String data=(event.getData() != null
                    // ? (event.getData().getData() != null ? new
                    // String(event.getData().getData(), "UTF-8") : "")
                    // : "");
                    // logger.info("treeCache data:{}", data);
                    if (childData == null || childData.getData() == null || childData.getData().length == 0) {
                        return;
                    }

                    if (childData != null) {
                        String path = childData.getPath();
                        if (path != null && path.length() > 0 && path.length() > zkpath.length()
                                && path.startsWith(zkpath) && path.lastIndexOf(".") > path.lastIndexOf("/")
                                && path.substring(path.lastIndexOf(".")).indexOf("-lock-") == -1) {
                            String tempPath = path.substring(zkpath.length() + 1);

                            String tempName = "";
                            String[] temps = tempPath.split("\\/");
                            tempPath = "";
                            if (temps.length > 0) {
                                int index = temps.length - 1;
                                if (temps[temps.length - 1].length() == 0) {
                                    index -= 1;
                                }
                                tempName = temps[index];

                                for (int j = 0; j < index; j++) {
                                    if (temps[j].length() > 0)
                                        tempPath = tempPath + temps[j] + "/";
                                }
                            }

                            if (tempName.length() == 0) {
                                return;
                            }

                            String filePath = classpath + tempPath + tempName;

                            if (classpath.indexOf("\\") > -1) {
                                filePath = filePath.replace("/", "\\");
                                tempPath = tempPath.replace("/", "\\");
                            }
                            logger.info("zkpath:{},path:{},filepath:{}", zkpath, path, filePath);

                            File file = new File(classpath + tempPath, tempName);
                            File backups = new File(webroot, "temp/backups");
                            if ((!backups.exists()) || (!backups.isDirectory())) {
                                backups.mkdir();
                            }
                            if (tempPath.length() > 0) {
                                backups = new File(webroot + "temp/backups" + File.separator + tempPath);
                                if ((!backups.exists()) || (!backups.isDirectory())) {
                                    backups.mkdir();
                                }
                            }

                            logger.info("backups path:{}", backups.getAbsoluteFile());

//							if (checkUpdate(childData.getPath(),childData.getStat().getMtime())){
                            if (file.exists()) {
                                if (checkUpdate(childData.getPath(), file.lastModified())) {
                                    File temp = new File(backups,
                                            UUID.randomUUID().toString() + tempName.substring(tempName.indexOf(".")));
                                    FileUtils.writeByteArrayToFile(temp, childData.getData(), false);
                                    logger.info("temp file:{},permissions:read={},write={}", temp.getAbsolutePath(),
                                            temp.canRead(), temp.canWrite());

                                    if (MD5Util.getFileMD5String(file).equals(MD5Util.getFileMD5String(temp))) {
                                        logger.info("path:{},localfile:{},changed:false,delete(temp):{}", path,
                                                file.getAbsolutePath(), temp.delete());
                                    } else {
                                        logger.info("path:{},localfile:{},changed:true", path, file.getAbsolutePath());

                                        String mypath = file.getAbsolutePath();

                                        boolean bl1 = file.renameTo(
                                                new File(backups, file.getName() + "." + UUID.randomUUID().toString()));
                                        boolean bl2 = temp.renameTo(file = new File(mypath));

                                        logger.info("path:{},localfile:{},change:true,update finished!back={},new={}", path,
                                                mypath, bl1, bl2);
                                    }

                                    updateFileLastMTime(childData.getPath(), file);
                                }
                            } else {
                                logger.info("path:{},localfile:{},add started!", path, filePath);
                                FileUtils.writeByteArrayToFile(file, childData.getData(), false);
                                logger.info("path:{},localfile:{},add finished!", path, file.getAbsolutePath());
                                updateFileLastMTime(childData.getPath(), file);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Exception:" + e.getMessage(), e);
                }
            }
        });

    }

    private static void reinit() {
        try {
            close();
            init();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public synchronized static void close() {
        try {

            if (treeCache != null) {
                treeCache.close();
                treeCache = null;

                logger.info("treeCache closed");
            }
            if (zkclient != null) {
                zkclient.close();
                zkclient = null;

                logger.info("zkclient closed");
            }

        } catch (Exception e) {
            logger.error("unregister failed:" + e.getMessage());
        } finally {
            state = false;
        }
    }

    /**
     * 创建或更新一个节点
     *
     * @param path    路径
     * @param content 内容
     **/
    private static boolean createrOrUpdateNode(String path, String content) throws Exception {
        return createrOrUpdateNode(path, content == null ? "".getBytes("UTF-8") : content.getBytes("UTF-8"));
    }

    /**
     * 创建或更新一个节点
     *
     * @param path    路径
     * @param content 内容
     **/
    private static boolean createrOrUpdateNode(String path, byte[] content) throws Exception {
        if (content == null) {
            content = "".getBytes("UTF-8");
        }
        try {
            Stat stat = zkclient.checkExists().creatingParentContainersIfNeeded().forPath(path);
            if (stat == null) {
                zkclient.create().creatingParentContainersIfNeeded().forPath(path, content);
                logger.info("create:" + path);
            } else {
                zkclient.setData().forPath(path, content);
                logger.info("update:" + path);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            logger.error("Exception:" + e.getMessage(), e);
            return false;
        }
        return true;
    }

    // /**
    // * 删除zk节点
    // *
    // * @param path
    // * 删除节点的路径
    // *
    // **/
    // private static boolean delete(String path) throws Exception {
    // try {
    // zkclient.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
    // logger.info("delete:" + path);
    // } catch (Exception e) {
    // // TODO Auto-generated catch block
    // logger.error("Exception:" + e.getMessage(), e);
    // return false;
    // }
    // return true;
    // }

    private static boolean checkDirectoryExists(String path) throws Exception {
        return zkclient.checkExists().forPath(path) != null;
    }

    private static boolean checkUpdate(String path, long mTime) throws Exception {
        return check(path, mTime, true);
    }

    private static boolean checkCommit(String path, long mTime) throws Exception {
        return check(path, mTime, false);
    }

    private static boolean updateFileLastMTime(String path, File file) throws Exception {
        DistributedAtomicLong dal = new DistributedAtomicLong(zkclient, "/history" + path, new ExponentialBackoffRetry(1000, 3));
        AtomicValue<Long> av = dal.get();
        if (av.succeeded() && av.postValue() != null && av.postValue().longValue() > 0L && av.postValue().longValue() != file.lastModified()) {
            logger.info("update file[{}] lastModified[{}]", file.getAbsolutePath(), av.postValue());
            return file.setLastModified(av.postValue());
        }
        return false;
    }

    private static boolean check(String path, long fileLastMTime, boolean updated) throws Exception {
        DistributedAtomicLong dal = new DistributedAtomicLong(zkclient, "/history" + path, new ExponentialBackoffRetry(1000, 3));
        AtomicValue<Long> av = dal.get();

        if (av.succeeded()) {
            Long preValue = av.preValue();
            Long postValue = av.postValue();
            if (updated) {
                if (postValue == null || postValue.longValue() <= fileLastMTime) {
                    logger.info("update:false,preValue={},postValue={},fileLastMTime={},path={}", preValue, postValue, fileLastMTime, path);
                    return false;
                } else {
                    logger.info("update:true,preValue={},postValue={},fileLastMTime={},path={}", preValue, postValue, fileLastMTime, path);
                    return true;
                }
            } else {
                if (postValue == null || postValue.longValue() < fileLastMTime) {
                    logger.info("commit:true,preValue={},postValue={},fileLastMTime={},path={}", preValue, postValue, fileLastMTime, path);
                    if (postValue == null || postValue.longValue() == 0L) {
                        av = dal.trySet(fileLastMTime);
                    } else {
                        av = dal.compareAndSet(postValue, fileLastMTime);
                    }

                    preValue = av.preValue();
                    postValue = av.postValue();
                    if (av.succeeded()) {
                        logger.info("commit:success,preValue={},postValue={},fileLastMTime={},path={}", preValue, postValue, fileLastMTime, path);
                    } else {
                        logger.info("commit:fail,preValue={},postValue={},fileLastMTime={},path={}", preValue, postValue, fileLastMTime, path);
                    }
                    return true;
                } else {
                    logger.info("commit:false,preValue={},postValue={},fileLastMTime={},path={}", preValue, postValue, fileLastMTime, path);
                    return false;
                }
            }
        } else {
            logger.error("check fail,fileLastMTime={},path={}", fileLastMTime, path);
            return false;
        }
    }


//	/**
//	 * 读取的路径
//	 *
//	 * @param path
//	 **/
//	private static String read(String path) throws Exception {
//		byte[] result = zkclient.getData().forPath(path);
//		if (result == null) {
//			return null;
//		}
//		String data = new String(result, "utf-8");
//		logger.debug("path:{},read data:{}", path, data);
//		return data;
//	}

    // /**
    // * @param path
    // * 路径 获取某个节点下的所有子文件
    // */
    // private static List<String> getListChildren(String path) throws Exception
    // {
    // List<String> paths = zkclient.getChildren().forPath(path);
    // for (String p : paths) {
    // logger.info(p);
    // }
    // return paths;
    // }

//	/**
//	 * @param classpath
//	 *            classpath相对路径
//	 * @param localpath
//	 *            本地上的文件路径
//	 *
//	 **/
//	public static boolean upload(String classpath, String localpath) {
//		String zkPath = loadZKpath(classpath);
//		try {
//
//			InterProcessMutex lock = new InterProcessMutex(zkclient, zkPath);
//			try {
//				if (lock.acquire(15, TimeUnit.MINUTES)) {
//					byte[] bs = FileUtils.readFileToByteArray(new File(localpath));
//
//					boolean result = createrOrUpdateNode(zkPath, bs);
//					logger.info("upload success.localpath={},zkpath={},result={}", localpath, zkPath, result);
//					return result;
//
//				}
//			} catch (Throwable t) {
//				logger.error("Throwable:" + t.getMessage(), t);
//			} finally {
//				lock.release();
//			}
//
//		} catch (Exception e) {
//			logger.error("Exception:" + e.getMessage(), e);
//		}
//		return false;
//	}

    // private static Stat updateData(String zkPath, String data) throws
    // Exception {
    // Stat stat = zkclient.setData().forPath(zkPath, data.getBytes("utf-8"));
    // logger.info("update success.zkpath={},data={},stat={}", zkPath, data,
    // stat);
    // return stat;
    // }

    /**
     * @param classpath classpath相对路径
     * @param localpath 本地上的文件路径
     **/
    public static boolean uploadIfNotExists(String classpath, String localpath) {
        String zkPath = loadZKpath(classpath);
        try {
            File file = new File(localpath);
            if (file.exists() && file.isFile() && checkCommit(zkPath, file.lastModified())) {
                InterProcessMutex lock = new InterProcessMutex(zkclient, zkPath);
                try {
                    if (lock.acquire(15, TimeUnit.MINUTES)) {
                        byte[] bs = FileUtils.readFileToByteArray(file);
                        boolean result = createrOrUpdateNode(zkPath, bs);
                        logger.info("upload success.localpath={},zkpath={},result={}", localpath, zkPath, result);
                        return result;

                    }
                } catch (Throwable t) {
                    logger.error("Throwable:" + t.getMessage(), t);
                } finally {
                    lock.release();
                }
            }
        } catch (Exception e) {
            logger.error("Exception:" + e.getMessage(), e);
        }
        return false;
    }

    private static String loadZKpath(String classpath) {
        final String webroot = PropertiesFileLoader.getWebRoot();
        String projectZKPath;
        if (classpath != null && classpath.trim().length() > 0) {
            projectZKPath = webroot + classpath;
        } else {
            projectZKPath = webroot;
        }

        if (projectZKPath.indexOf(":") > -1) {
            projectZKPath = projectZKPath.substring(projectZKPath.indexOf(":") + 1);
        }
        projectZKPath = projectZKPath.replace("\\", "/");
        if (projectZKPath.indexOf("\\") > -1) {
            projectZKPath = projectZKPath.replace("\\", "/");
        }
        if (projectZKPath.endsWith("/")) {
            projectZKPath = projectZKPath.substring(0, projectZKPath.length() - 1);
        }

        String zkroot = PropertiesCacheUtil.getValue("zkroot", ZOOKEEPER_FILE);

        StringBuilder stringBuilder = new StringBuilder();

        if (zkroot != null && zkroot.trim().length() > 0) {
            stringBuilder.append("/").append(zkroot);
        }
        return stringBuilder.append("/").append(CacheUtil.getCachePrefix()).append(projectZKPath).toString();
    }

}
