MongoDB-Driver-For-ThinkPHP3.2
===========================
****
### Author:hector
### E-mail:hectorqin@163.com
****
This repository is modified from [admpub/MongoDB-Driver-for-ThinkPHP](https://github.com/admpub/MongoDB-Driver-for-ThinkPHP), adapted for ThinkPHP3.2. It requres the new driver of mongodb that includes PHPLIBs and the php extension of mongodb

##usage
Put the model and db driver into the right folder, and write your model file extends 'MongodbModel'.For example:
```PHP
namespace Home\Model;
use Think\Model\MongodbModel;
Class UserModel extends MongodbModel {
     Protected $_idType = self::TYPE_INT;
     protected $_autoinc =  true;
}
```
Make sure set the db_type option in the config or the connection property of the model to 'mongodb'.