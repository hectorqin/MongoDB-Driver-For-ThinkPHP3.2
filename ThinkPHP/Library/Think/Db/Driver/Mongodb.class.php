<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK IT ]
// +----------------------------------------------------------------------
// | Copyright (c) 2006-2014 http://thinkphp.cn All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
// +----------------------------------------------------------------------

namespace Think\Db\Driver;
use Think\Db\Driver;
use Think\Log;

/**
 * Mongodb数据库驱动 必须配合MongoModel使用
 */
class Mongodb extends Driver {

    protected $_database = null; // MongoDb Object
    protected $_collection = null; // MongoCollection Object
    protected $_dbName = ''; // dbName
    protected $_collectionName = ''; // collectionName
    protected $_cursor = null; // MongoCursor Object
    protected $comparison = array('neq' => 'ne', 'ne' => 'ne', 'gt' => 'gt', 'egt' => 'gte', 'gte' => 'gte', 'lt' => 'lt', 'elt' => 'lte', 'lte' => 'lte', 'in' => 'in', 'not in' => 'nin', 'nin' => 'nin');

    /**
     * 架构函数 读取数据库配置信息
     * @access public
     * @param array $config 数据库配置数组
     */
    public function __construct($config = '') {
        if (!class_exists('\MongoDB\Driver\Manager')) {
            E(L('_NOT_SUPPERT_') . ':\MongoDB\Driver\Manager');
        }
        if (!empty($config)) {
            $this->config = $config;
            if (empty($this->config['params'])) {
                $this->config['params'] = array();
            }
        }
    }

    /**
     * 连接数据库方法
     * @access public
     */
    public function connect($config = '', $linkNum = 0) {
        if (!isset($this->linkID[$linkNum])) {
            if (empty($config)) $config = $this->config;
            $host = 'mongodb://' . ($config['username'] ? "{$config['username']}" : '') . ($config['password'] ? ":{$config['password']}@" : '') . $config['hostname'] . ($config['hostport'] ? ":{$config['hostport']}" : '') . '/' . ($config['database'] ? "{$config['database']}" : '');

            try {
                $this->linkID[$linkNum] = new \MongoDB\Client($host, $config['params']);
            } catch (\Exception $e) {
                E($e->getMessage());
            }
            // 标记连接成功
            $this->connected = true;
        }
        return $this->linkID[$linkNum];
    }

    /**
     * 切换当前操作的Db和Collection
     * @access public
     * @param string $collection collection
     * @param string $db db
     * @param boolean $master 是否主服务器
     * @return void
     */
    public function switchCollection($collection, $db = '', $master = true) {
        $this->initConnect($master);
        try {
            if (!empty($db) && $this->_dbName != $db) { // 传人Db则切换数据库
                if($this->config['debug']) {
                    $this->queryStr = $this->_dbName . '.selectDB(' . $db . ')';
                }
                $this->_dbName = $db;
                $this->_database = $this->_linkID->selectDatabase($db);
            }
            if (!empty($collection) && $this->_collectionName != $collection) {
                if($this->config['debug']) {
                    $this->queryStr = $this->_dbName . '.getCollection(' . $collection . ')';
                }
                N('db_read', 1);
                $this->debug(true);
                $this->_collection = $this->_database->selectCollection($collection);
                $this->debug(false);
                $this->_collectionName = $collection; // 记录当前Collection名称
            }
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    protected function _namespace() {
        return $this->_dbName . '.' . $this->_collectionName;
    }

    public function mongoId($id) {
        return new \MongoDB\BSON\ObjectID($id);
    }

    public function mongoCode($code, $args = array()) {
        return new \MongoDB\BSON\Javascript($code, $args);
    }

    /**
     * 释放查询结果
     * @access public
     */
    public function free() {
        $this->_cursor = null;
    }

    /**
     * 执行命令
     * @access public
     * @param array $command 指令
     * @return array
     */
    public function command($command = array()) {
        N('db_write', 1);
        $this->queryStr = 'command:' . json_encode($command);
        $this->debug(true);
        try {
            $result = $this->_database->command($command);
        } catch (\Exception $e) {
            E($e->getMessage());
        }
        $this->debug(false);
        return $result;
    }

    /**
     * 执行语句 (在 MongoDB3.0+ 已作废)
     * @access public
     * @param string $code sql指令
     * @param array $args 参数
     * @return mixed

    public function execute($code,$args=array()) {
     * N('db_write',1);
     * $this->queryStr = 'execute:'.$code;
     * // 记录开始执行时间
     * G('queryStartTime');
     * try{
     * $result   = $this->_database->execute($code,$args);
     * } catch(\Exception $e) {
     * E($e->getMessage());
     * }
     * $this->debug(false);
     * return $result;
     * }
     */

    /**
     * 关闭数据库
     * @access public
     */
    public function close() {
        if ($this->_linkID) {
            $this->_linkID = null;
            $this->_database = null;
            $this->_collection = null;
            $this->_dbName = '';
            $this->_collectionName = '';
            $this->_cursor = null;
        }
    }

    /**
     * 数据库错误信息
     * @access public
     * @return string
     */
    public function error() {
        //$this->error = $this->_data->lastError();
        trace($this->error, '', 'ERR');
        return $this->error;
    }

    /**
     * 插入记录
     * @access public
     * @param mixed $data 数据
     * @param array $options 参数表达式
     * @param boolean $replace 是否replace
     * @return false | integer
     */
    public function insert($data, $options = array(), $replace = false) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database']);
        }
        $this->model = $options['model'];
        N('db_write', 1);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.insert(';
            $this->queryStr .= $data ? json_encode($data) : '{}';
            $this->queryStr .= ')';
        }
        try {
            $this->debug(true);
            $opts = isset($options['options']) ? $options['options'] : array();
            $result = $replace ? $this->_collection->findOneAndReplace(array(
                '_id' => is_object($data) ? $data->_id : $data['_id']
            ), $data, $opts) : $this->_collection->insertOne($data, $opts);
            $this->debug(false);
            if ($result) {
                if ($result instanceof \MongoDB\InsertOneResult) {
                    $this->lastInsID = $result->getInsertedId()->__toString();
                    $this->numRows = $result->getInsertedCount();
                } else {
                    $_id = is_object($result) ? $result->_id : $result['_id'];
                    if (is_object($_id)) {
                        $_id = $_id->__toString();
                    }
                    $this->lastInsID = $_id;
                }
            }
            return $result;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 插入多条记录
     * @access public
     * @param array $dataList 数据
     * @param array $options 参数表达式
     * @return bool
     */
    public function insertAll($dataList, $options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database']);
        }
        $this->model = $options['model'];
        N('db_write', 1);
        try {
            $this->debug(true);
            $opts = isset($options['options']) ? $options['options'] : array();
            $result = $this->_collection->insertMany($dataList, $opts);
            $this->debug(false);
            return $result;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 生成下一条记录ID 用于自增非MongoId主键
     * @access public
     * @param string $pk 主键名
     * @return integer
     */
    public function mongo_next_id($pk) {
        N('db_read', 1);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.find({},{' . $pk . ':1}).sort({' . $pk . ':-1}).limit(1)';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->find(array(), array(
                'sort' => array($pk => -1),
                'limit' => 1
            ));
            $this->debug(false);
        } catch (\Exception $e) {
            E($e->getMessage());
        }
        $data = array();
        if ($result) {
            $data = reset($result);
        }
        return isset($data[$pk]) ? $data[$pk] + 1 : 1;
    }

    /**
     * 更新记录
     * @access public
     * @param mixed $data 数据
     * @param array $options 表达式
     * @return bool
     */
    public function update($data, $options) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database']);
        }
        $this->model = $options['model'];
        N('db_write', 1);
        $query = $this->parseWhere($options['where']);
        $set = $this->parseSet($data);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.update(';
            $this->queryStr .= $query ? json_encode($query) : '{}';
            $this->queryStr .= ',' . json_encode($set) . ')';
        }
        try {
            $this->debug(true);
            $opts = isset($options['options']) ? $options['options'] : array();
            if (isset($options['limit']) && $options['limit'] == 1) {
                $result = $this->_collection->updateOne($query, $set, $opts);
            } else {
                $result = $this->_collection->updateMany($query, $set, $opts);
            }
            if ($result) {
                $this->numRows = $result->getModifiedCount();
                $result = $this->numRows;
            }
            $this->debug(false);
            return $result;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 删除记录
     * @access public
     * @param array $options 表达式
     * @return false | integer
     */
    public function delete($options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database']);
        }
        $query = $this->parseWhere($options['where']);
        $this->model = $options['model'];
        N('db_write', 1);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.remove(' . json_encode($query) . ')';
        }
        try {
            $this->debug(true);
            $opts = isset($options['options']) ? $options['options'] : array();
            if (isset($options['limit']) && $options['limit'] == 1) {
                $result = $this->_collection->deleteOne($query, $opts);
            } else {
                if (!$query) E('没有指定数据的删除条件，这会导致全表被清空，不允许这么操作。');
                $result = $this->_collection->deleteMany($query, $opts);
            }

            if ($result) {
                $this->numRows = $result->getDeletedCount();
                $result = $this->numRows;
            }
            $this->debug(false);
            return $result;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 清空记录
     * @access public
     * @param array $options 表达式
     * @return false | integer
     */
    public function clear($options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database']);
        }
        $this->model = $options['model'];
        N('db_write', 1);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.remove({})';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->drop();
            $this->debug(false);
            return $result;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 查找记录
     * @access public
     * @param array $options 表达式
     * @return iterator
     */
    public function select($options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database'], false);
        }
        $cache = isset($options['cache']) ? $options['cache'] : false;
        if ($cache) { // 查询缓存检测
            $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            $value = S($key, '', $cache);
            if (false !== $value) {
                return $value;
            }
        }
        $this->model = $options['model'];
        N('db_query', 1);
        $query = $this->parseWhere($options['where']);
        $field = $this->parseField($options['field']);
        try {
            if($this->config['debug']) {
                if (isset($options['distinct'])) {
                    $this->queryStr = $this->_namespace() . '.distinct("' . $options['distinct'] . '",';
                    $this->queryStr .= $query ? json_encode($query) : '{}';
                    $this->queryStr .= ')';
                } else {
                    $this->queryStr = $this->_namespace() . '.find(';
                    $this->queryStr .= $query ? json_encode($query) : '{}';
                    $this->queryStr .= $field ? ',' . json_encode($field) : '';
                    $this->queryStr .= ')';
                }
            }
            $this->debug(true);
            $opts = isset($options['options']) ? $options['options'] : array();
            if ($field) $opts['projection'] = $field;
            if ($options['order']) {
                $order = $this->parseOrder($options['order']);
                if($this->config['debug']) {
                    $this->queryStr .= '.sort(' . json_encode($order) . ')';
                }
                $opts['sort'] = $order;
            }
            if (isset($options['page'])) { // 根据页数计算limit
                if (strpos($options['page'], ',')) {
                    list($page, $length) = explode(',', $options['page']);
                } else {
                    $page = $options['page'];
                }
                $page = $page ? $page : 1;
                $length = isset($length) ? $length : (is_numeric($options['limit']) ? $options['limit'] : 20);
                $offset = $length * ((int)$page - 1);
                $options['limit'] = $offset . ',' . $length;
            }
            if (isset($options['limit'])) {
                list($offset, $length) = $this->parseLimit($options['limit']);
                if (!empty($offset)) {
                    if($this->config['debug']) {
                        $this->queryStr .= '.skip(' . intval($offset) . ')';
                    }
                    $opts['skip'] = intval($offset);
                }
                if($this->config['debug']) {
                    $this->queryStr .= '.limit(' . intval($length) . ')';
                }
                $opts['limit'] = intval($length);
                if ($opts['limit'] < 1) $opts['limit'] = 20;
            }
            if (isset($options['distinct'])) {
                $_cursor = $this->_collection->distinct($options['distinct'], $query, $opts);
                $resultSet = $_cursor;//array(123,445,666)
            } else {
                $_cursor = $this->_collection->find($query, $opts);
                $resultSet = iterator_to_array($_cursor);
            }
            $this->_cursor = $_cursor;
            $this->debug(false);
            if ($cache && $resultSet) { // 查询缓存写入
                S($key, $resultSet, $cache);
            }
            return $resultSet;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 查找某个记录
     * @access public
     * @param array $options 表达式
     * @return array
     */
    public function find($options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database'], false);
        }
        $cache = isset($options['cache']) ? $options['cache'] : false;
        if ($cache) { // 查询缓存检测
            $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            $value = S($key, '', $cache);
            if (false !== $value) {
                return $value;
            }
        }
        $this->model = $options['model'];
        N('db_query', 1);
        $query = $this->parseWhere($options['where']);
        $fields = $this->parseField($options['field']);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.findOne(';
            $this->queryStr .= $query ? json_encode($query) : '{}';
            $this->queryStr .= $fields ? ',' . json_encode($fields) : '';
            $this->queryStr .= ')';
        }
        try {
            $opts = isset($options['options']) ? $options['options'] : array();
            $this->debug(true);
            if ($fields) $opts['projection'] = $fields;
            $result = $this->_collection->findOne($query, $opts);
            $this->debug(false);
            if ($cache && $result) { // 查询缓存写入
                S($key, $result, $cache);
            }
            return $result;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    /**
     * 统计记录数
     * @access public
     * @param array $options 表达式
     * @return iterator
     */
    public function count($options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database'], false);
        }
        $cache = isset($options['cache']) ? $options['cache'] : false;
        if ($cache) { // 查询缓存检测
            $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            $value = S($key, '', $cache);
            if (false !== $value) {
                return $value;
            }
        }
        $this->model = $options['model'];
        N('db_query', 1);
        $query = $this->parseWhere($options['where']);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace();
            $this->queryStr .= $query ? '.find(' . json_encode($query) . ')' : '';
            $this->queryStr .= '.count()';
        }
        try {
            $this->debug(true);
            $opts = isset($options['options']) ? $options['options'] : array();
            $count = $this->_collection->count($query, $opts);
            $this->debug(false);
            if ($cache && is_numeric($count)) { // 查询缓存写入
                S($key, $count, $cache);
            }
            return $count;
        } catch (\Exception $e) {
            E($e->getMessage());
        }
    }

    public function group($keys, $initial, $reduce, $options = array()) {
        $this->_collection->group($keys, $initial, $reduce, $options);
    }

    /**
     * 取得数据表的字段信息
     * @access public
     * @return array
     */
    public function getFields($collection = '', $database = '') {
        $this->switchCollection($collection, $database, false);
        N('db_query', 1);
        if($this->config['debug']) {
            $this->queryStr = $this->_namespace() . '.findOne()';
        }
        try {
            $this->debug(true);
            $result = $this->_collection->findOne();
            $this->debug(false);
        } catch (\Exception $e) {
            E($e->getMessage());
        }
        if ($result) { // 存在数据则分析字段
            $info = array();
            foreach ($result as $key => $val) {
                $info[$key] = array(
                    'name' => $key,
                    'type' => getType($val),
                );
            }
            return $info;
        }
        // 暂时没有数据 返回false
        return false;
    }

    /**
     * 取得当前数据库的collection信息
     * @access public
     */
    public function getTables($database = '') {
        $this->switchCollection('', $database, false);
        if($this->config['debug']) {
            $this->queryStr = $this->_dbName . '.getCollenctionNames()';
        }
        N('db_query', 1);
        $this->debug(true);
        $list = $this->_database->listCollections();
        $this->debug(false);
        $info = array();
        foreach ($list as $collection) {
            $info[] = $collection->getName();
        }
        return $info;
    }

    /**
     * set分析
     * @access protected
     * @param array $data
     * @return string
     */
    protected function parseSet($data) {
        $result = array();
        foreach ($data as $key => $val) {
            if (is_array($val)) {
                switch ($val[0]) {
                    case 'inc':
                        $result['$inc'][$key] = (int)$val[1];
                        break;
                    case 'set':
                    case 'unset':
                    case 'push':
                    case 'pushall':
                    case 'addtoset':
                    case 'pop':
                    case 'pull':
                    case 'pullall':
                        $result['$' . $val[0]][$key] = $val[1];
                        break;
                    default:
                        $result['$set'][$key] = $val;
                }
            } else {
                $result['$set'][$key] = $val;
            }
        }
        return $result;
    }

    /**
     * order分析
     * @access protected
     * @param mixed $order
     * @return array
     */
    protected function parseOrder($order) {
        if (is_string($order)) {
            $array = explode(',', $order);
            $order = array();
            foreach ($array as $key => $val) {
                $arr = explode(' ', trim($val));
                if (isset($arr[1])) {
                    $arr[1] = trim($arr[1]);
                    $arr[1] = strtoupper($arr[1]) == 'ASC' ? 1 : -1;
                } else {
                    $arr[1] = 1;
                }
                $arr[0] = trim($arr[0]);
                $order[$arr[0]] = $arr[1];
            }
        }
        return $order;
    }

    /**
     * limit分析
     * @access protected
     * @param mixed $limit
     * @return array
     */
    protected function parseLimit($limit) {
        if (strpos($limit, ',')) {
            $array = explode(',', $limit);
        } else {
            $array = array(0, $limit);
        }
        return $array;
    }

    /**
     * field分析
     * @access protected
     * @param mixed $fields
     * @return array
     */
    public function parseField($fields) {
        if (empty($fields)) {
            $fields = array();
        }
        if (is_string($fields)) {
            $fields = explode(',', $fields);
        }
        $projection = array();
        foreach ($fields as $field => $show) {
            if (is_numeric($show) || is_bool($show)) {
                $projection[$field] = $show;
                continue;
            }
            $field = $show;
            $field = trim($field);
            $projection[$field] = 1;
        }
        return $projection;
    }

    /**
     * where分析
     * @access protected
     * @param mixed $where
     * @return array
     */
    public function parseWhere($where) {
        $query = array();
        foreach ($where as $key => $val) {
            if ('_id' != $key && 0 === strpos($key, '_')) {
                // 解析特殊条件表达式
                $query = $this->parseThinkWhere($key, $val);
            } else {
                // 查询字段的安全过滤
                if (!preg_match('/^[A-Z_\|\&\-.a-z0-9]+$/', trim($key))) {
                    E(L('_ERROR_QUERY_') . ':' . $key);
                }
                $key = trim($key);
                if (strpos($key, '|')) {
                    $array = explode('|', $key);
                    $str = array();
                    foreach ($array as $k) {
                        $str[] = $this->parseWhereItem($k, $val);
                    }
                    $query['$or'] = $str;
                } elseif (strpos($key, '&')) {
                    $array = explode('&', $key);
                    $str = array();
                    foreach ($array as $k) {
                        $str[] = $this->parseWhereItem($k, $val);
                    }
                    $query = array_merge($query, $str);
                } else {
                    $str = $this->parseWhereItem($key, $val);
                    $query = array_merge($query, $str);
                }
            }
        }
        return $query;
    }

    /**
     * 特殊条件分析
     * @access protected
     * @param string $key
     * @param mixed $val
     * @return string
     */
    protected function parseThinkWhere($key, $val) {
        $query = array();
        switch ($key) {
            case '_query': // 字符串模式查询条件
                parse_str($val, $query);
                if (isset($query['_logic']) && strtolower($query['_logic']) == 'or') {
                    unset($query['_logic']);
                    $query['$or'] = $query;
                }
                break;
            case '_string':// MongoCode查询
                $query['$where'] = new \MongoDB\BSON\Javascript($val);
                break;
            case '_where':
                $query = $val;
                break;
        }
        return $query;
    }

    /**
     * where子单元分析
     * @access protected
     * @param string $key
     * @param mixed $val
     * @return array
     */
    protected function parseWhereItem($key, $val) {
        $query = array();
        if (is_array($val)) {
            if (is_string($val[0])) {
                $con = strtolower($val[0]);
                if (in_array($con, array('neq', 'ne', 'gt', 'egt', 'gte', 'lt', 'lte', 'elt'))) { // 比较运算
                    $k = '$' . $this->comparison[$con];
                    $query[$key] = array($k => $val[1]);
                } elseif ('like' == $con) { // 模糊查询 采用正则方式
                    $query[$key] = new \MongoDB\BSON\Regex("/" . $val[1] . "/");
                } elseif ('mod' == $con) { // mod 查询
                    $query[$key] = array('$mod' => $val[1]);
                } elseif ('regex' == $con) { // 正则查询
                    $query[$key] = new \MongoDB\BSON\Regex($val[1]);
                } elseif (in_array($con, array('in', 'nin', 'not in'))) { // IN NIN 运算
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $k = '$' . $this->comparison[$con];
                    $query[$key] = array($k => $data);
                } elseif ('all' == $con) { // 满足所有指定条件
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $query[$key] = array('$all' => $data);
                } elseif ('between' == $con) { // BETWEEN运算
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $query[$key] = array('$gte' => $data[0], '$lte' => $data[1]);
                } elseif ('not between' == $con) {
                    $data = is_string($val[1]) ? explode(',', $val[1]) : $val[1];
                    $query[$key] = array('$lt' => $data[0], '$gt' => $data[1]);
                } elseif ('exp' == $con) { // 表达式查询
                    $query['$where'] = new \MongoDB\BSON\Javascript($val[1]);
                } elseif ('exists' == $con) { // 字段是否存在
                    $query[$key] = array('$exists' => (bool)$val[1]);
                } elseif ('size' == $con) { // 限制属性大小
                    $query[$key] = array('$size' => intval($val[1]));
                } elseif ('type' == $con) { // 限制字段类型 1 浮点型 2 字符型 3 对象或者MongoDBRef 5 MongoBinData 7 MongoId 8 布尔型 9 MongoDate 10 NULL 15 MongoCode 16 32位整型 17 MongoTimestamp 18 MongoInt64 如果是数组的话判断元素的类型
                    $query[$key] = array('$type' => intval($val[1]));
                } else {
                    $query[$key] = $val;
                }
                return $query;
            }
        }
        $query[$key] = $val;
        return $query;
    }

    /**
     * 创建索引
     * @param array $key 索引设置，例如：array('username'=>1)
     * @param array $options 选项，例如：array('options'=>array('unique' => true))
     * @return string 返回创建的索引名称
     */
    public function createIndex($key, $options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database'], true);
        }
        $this->model = $options['model'];
        $multiple = isset($options['multiple']) && $options['multiple'];
        $opts = isset($options['options']) ? $options['options'] : array();

        /**
         * $multiple为true时$key的值格式应为：
         * array(
         *    // Create a unique index on the "username" field
         *    array( 'key' => array( 'username' => 1 ), 'unique' => true ),
         *    // Create a 2dsphere index on the "loc" field with a custom name
         *    array( 'key' => array( 'loc' => '2dsphere' ), 'name' => 'geo' ),
         * )
         */
        if($multiple){
            if(is_string($key)) $key = $this->_parseIndexSetting($key);
            return $this->_collection->createIndexes($key);
        }
        return $this->_collection->createIndex($key, $opts);
    }

    /**
     * 从字符串中获取索引设置，例如：unique:username,-userid;
     * @param string $key
     * @return array
     */
    private function _parseIndexSetting($key=''){
        $indexes=explode(';',trim($key));
        $key=array();
        foreach($indexes as $index){
            $index=trim($index);
            if(!$index)continue;
            $vi=explode(':',$index);
            $type='';
            if(isset($vi[1])){
                $type=trim($vi[0]);
                $_fields=explode(',',trim($vi[1]));
            }else{
                $_fields=explode(',',trim($vi[0]));
            }
            $fields=array();
            foreach($_fields as $field){
                $field=trim($field);
                if(!$field)continue;
                if($field[0]=='-'){
                    $fields[]=array(substr($field,1)=>-1);
                }elseif($field[0]=='+'){
                    $fields[]=array(substr($field,1)=>1);
                }else{
                    $fields[]=array($field=>1);
                }
            }
            $keyset=array('key'=>$fields);
            if($type)$keyset[$type]=true;
            $key[]=$keyset;
        }
        return $key;
    }

    /**
     * 创建表
     * @param string $table 表名称
     * @param array $options 选项
     */
    public function createTable($table, $options = array()) {
        $this->switchCollection($options['table'], $options['database'], true);
        $opts = isset($options['options']) ? $options['options'] : array();
        $this->_database->createCollection($table, $opts);
    }

    /**
     * 删除索引
     * @param string $indexName 索引名称
     * @param array $options 选项
     * @return string 返回创建的索引名称
     */
    public function dropIndex($indexName, $options = array()) {
        if (isset($options['table'])) {
            $this->switchCollection($options['table'], $options['database'], true);
        }
        $this->model = $options['model'];
        $opts = isset($options['options']) ? $options['options'] : array();
        return $this->_collection->dropIndex($key, $opts);
    }

    /**
     * 删除表
     * @param string $table 表名称
     * @param array $options 选项
     */
    public function dropTable($table, $options = array()) {
        $this->switchCollection($options['table'], $options['database'], true);
        $opts = isset($options['options']) ? $options['options'] : array();
        $this->_database->dropCollection($table, $opts);
    }
}
