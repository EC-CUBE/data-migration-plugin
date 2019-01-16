<?php

namespace Plugin\DataMigration4\Controller\Admin;

use Eccube\Controller\AbstractController;
use Eccube\Service\PluginService;
use Eccube\Util\StringUtil;
use Plugin\DataMigration4\Form\Type\Admin\ConfigType;
use Plugin\DataMigration4\Util\BulkInsertQuery;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Template;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Filesystem\Filesystem;
use Doctrine\DBAL\Driver\Connection;
use \wapmorgan\UnifiedArchive\UnifiedArchive;

class ConfigController extends AbstractController
{
    /** @var pluginService */
    protected $pluginService;

    /**
     * constructor.
     *
     * @param pluginService $pluginService
     */
    public function __construct(
        pluginService $pluginService
    ) {
        $this->pluginService = $pluginService;
    }


    /**
     * @Route("/%eccube_admin_route%/data_migration4/config", name="data_migration4_admin_config")
     * @Template("@DataMigration4/admin/config.twig")
     */
    public function index(Request $request, Connection $em)
    {
        $this->delivery_id = array();
        $this->stock = array();
        $this->shipping_id = array();
        $this->product_class_id = array();
        $this->order_item = array();

        $form = $this->createForm(ConfigType::class);
        $form->handleRequest($request);

        if ($form->isSubmitted() && $form->isValid()) {

            // logをオフにしてメモリを減らす
            $em->getConfiguration()->setSQLLogger(null);

            $formFile = $form['import_file']->getData();

            $tmpFile = $formFile->getClientOriginalName();
            $tmpDir = $this->pluginService->createTempDir();
            $formFile->move($tmpDir, $tmpFile);

            $archive = UnifiedArchive::open($tmpDir.'/'.$tmpFile);
            // 解凍
            $archive->extractNode($tmpDir);
            $fileNames = $archive->getFileNames();

            // 2.4.4系の場合の処理
            if ($archive->getFileData($fileNames[0].'bkup_data.csv')) {
                $csvDir = $tmpDir.'/'.$fileNames[0];
                $this->cutOff24($csvDir, 'bkup_data.csv');

                // 税率など
                $this->fix24baseinfo($em, $csvDir);
                // create dtb_shipping
                $this->fix24Shipping($em, $csvDir);

                // 2.4.4系の場合の処理
                if (file_exists($csvDir.'dtb_products_class.csv')) {
                    // 2.11の場合は通さない
                    if (!file_exists($csvDir.'dtb_class_combination.csv')) {
                        $this->fix24ProductsClass($em, $csvDir);
                    }
                }

            } else {
                $csvDir = $tmpDir . '/';
            }
            $this->saveCustomer($em, $csvDir);
            $this->saveProduct($em, $csvDir);
            $this->saveOrder($em, $csvDir);

            // todo 送料など

            // 削除
            $fs = new Filesystem();
            $fs->remove($tmpDir);

            return $this->redirectToRoute('data_migration4_admin_config');
        }

        return [
            'form' => $form->createView(),
        ];
    }


    private function saveCustomer($em, $csvDir) {

        // 会員系
        if (file_exists($csvDir.'dtb_customer.csv') && filesize($csvDir.'dtb_customer.csv') > 0 ) {

            $em->beginTransaction();

            $platform = $em->getDatabasePlatform()->getName();

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
                $em->exec("SET SESSION sql_mode = ''"); // STRICT_TRANS_TABLESを無効にする。
            } else {
                $em->exec("SET session_replication_role = replica;"); // need super user
            }

            $this->saveToC($em, $csvDir, 'mtb_job', null, true);
            $this->saveToC($em, $csvDir, 'mtb_sex', null, true);
            $this->saveToC($em, $csvDir, 'dtb_customer');
            //$index = $this->saveTo($em, $csvDir, 'dtb_customer', 'dtb_customer_address'); // 3と仕様が違う
            $this->saveToC($em, $csvDir, 'dtb_other_deliv', 'dtb_customer_address', false, 1/*$index*/);

            //$this->saveToC($em, $csvDir, 'mtb_authority', null, true);
            //$this->saveToC($em, $csvDir, 'dtb_member', null, true);

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 1;');
            } else {
                $this->setIdSeq($em, 'dtb_member');
                $this->setIdSeq($em, 'dtb_customer');
                $this->setIdSeq($em, 'dtb_customer_address');
            }
            $em->commit();

            $this->addSuccess('会員データ登録しました。', 'admin');
        } else {
            $this->addDanger('会員データが見つかりませんでした', 'admin');
        }
    }

    private function saveToC($em, $tmpDir, $csvName, $tableName = null, $allow_zero = false, $i = 1) {

        $tableName = ($tableName)?$tableName:$csvName;
        $em->exec('DELETE FROM ' . $tableName);

        if (file_exists($tmpDir.$csvName.'.csv') == false) {
            // 無視する
            //$this->addDanger($csvName.'.csv が見つかりませんでした' , 'admin');
            return;
        }
        if (filesize($tmpDir.$csvName.'.csv') == 0) {
            // 無視する
            return;
        }

        if (($handle = fopen($tmpDir.$csvName.'.csv', "r")) !== FALSE) {

            // 文字コード問題が起きる可能性が高いので後で調整が必要になると思う
            $key = fgetcsv($handle);
            $keySize = count($key);

            $columns = $em->getSchemaManager()->listTableColumns($tableName);
            foreach ($columns as $column) {
                $listTableColumns[] =  $column->getName();
            }

            $builder = new BulkInsertQuery($em, $tableName, 20);
            $builder->setColumns($listTableColumns);

            $batchSize = 20;

            while(($row = fgetcsv($handle)) !== FALSE) {
                $value = array();

                // 1行目をkeyとした配列を作る
                $data = array_combine($key, $row);

                // 物理削除になったので
                if (isset($data['del_flg']) && $data['del_flg'] == 1) continue;

                // Schemaにあわせた配列を作成する
                foreach($listTableColumns as $column) {
                    if ($column == 'id' && $tableName == 'dtb_customer') { // fixme
                        $value[$column] = $data['customer_id'];

                    } elseif ($column == 'customer_status_id') {
                        // 退会が追加された
                        $value[$column] = ($data['del_flg'] == 1) ? '3' : $data['status'];

                    } elseif ($column == 'postal_code') {
                        $value[$column] = mb_substr(mb_convert_kana($data['zip01'].$data['zip02'], 'a'), 0 , 8); //
                        if (empty($value[$column])) { $value[$column] = NULL;}
                    } elseif ($column == 'phone_number') {
                        $value[$column] = mb_substr(mb_convert_kana($data['tel01'].$data['tel02'].$data['tel03'], 'a'), 0 , 14); //14文字制限
                        if (empty($value[$column])) { $value[$column] = NULL;}
                    } elseif ($column == 'sex_id') {
                        $value[$column] = empty($data['sex']) ? NULL : $data['sex'];
                    } elseif ($column == 'job_id') {
                        $value[$column] = empty($data['job']) ? NULL : $data['job']; // 0が入っている場合あり?
                    } elseif ($column == 'pref_id') {
                        $value[$column] = empty($data['pref']) ? NULL : $data['pref'];
                    } elseif ($column == 'work_id') {
                        $value[$column] = $data['work'];
                    } elseif ($column == 'authority_id') {
                        $value[$column] = $data['authority'];
                    } elseif ($column == 'email' || $column == 'password' || $column == 'name01' || $column == 'name02') {
                        $value[$column] = empty($data[$column]) ? 'Not null violation' : $data[$column];
                    } elseif ($column == 'sort_no') {
                        $value[$column] = $data['rank'];

                    } elseif ($column == 'create_date' || $column == 'update_date') {
                        $value[$column] = (isset($data[$column])&& $data[$column] != '0000-00-00 00:00:00')?$data[$column]:date('Y-m-d H:i:s');
                    } elseif ($column == 'login_date' || $column == 'first_buy_date') {
                        $value[$column] = (!empty($data[$column])&& $data[$column] != '0000-00-00 00:00:00')?$data[$column]:NULL;
                    } elseif ($column == 'secret_key') { // 実験
                        $value[$column] = mt_rand();
                    } elseif ($column == 'point') {
                        $value[$column] = !empty($data[$column])?$data[$column]:0;
                    } elseif ($column == 'salt') {
                        $value[$column] = !empty($data[$column])?$data[$column]:''; // not null
                    } elseif ($column == 'creator_id') {
                        $value[$column] = !empty($data[$column])?$data[$column]:1;

                    } elseif ($column == 'id' && $tableName == 'dtb_member') {
                        $value[$column] = $data['member_id'];

                    } elseif ($column == 'id' && $tableName == 'dtb_customer_address') {
                            // カラム名が違うので
                        $value[$column] = $i;
                    } elseif ($column == 'discriminator_type') {
                        $search = array('dtb_','mtb_', '_');
                        $value[$column] = str_replace($search, '', $tableName);
                    } elseif ($allow_zero) {
                        $value[$column] = isset($data[$column])?$data[$column]:NULL;
                    } else {
                        $value[$column] = !empty($data[$column])?$data[$column]:NULL;
                    }
                }
                $builder->setValues($value);

                if (($i % $batchSize) === 0) {
                    $builder->execute();
                }

                $i++;
            }

            if (count($builder->getValues()) > 0) {
                $builder->execute();
            }

            fclose($handle);
            return $i; // indexを返す
        }
    }


    private function saveProduct($em, $csvDir){

        if (file_exists($csvDir.'dtb_products.csv') && filesize($csvDir.'dtb_products.csv') > 0 ) {
            $em->beginTransaction();

            $platform = $em->getDatabasePlatform()->getName();

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
                $em->exec("SET SESSION sql_mode = ''"); // STRICT_TRANS_TABLESを無効にする。
            } else {
                $em->exec("SET session_replication_role = replica;");
            }

            // 2.11系の処理
            if (file_exists($csvDir.'dtb_class_combination.csv')) {
                $this->fix211classCombination($em, $platform, $csvDir);
            }

            $this->saveToP($em, $csvDir, 'dtb_products', 'dtb_product');
            $this->saveToP($em, $csvDir, 'dtb_products_class', 'dtb_product_class');
            $this->saveToP($em, $csvDir, 'dtb_classcategory', 'dtb_class_category');
            $this->saveToP($em, $csvDir, 'dtb_class', 'dtb_class_name');
            $this->saveToP($em, $csvDir, 'dtb_category');
            $this->saveToP($em, $csvDir, 'dtb_product_categories', 'dtb_product_category');

            $this->saveToP($em, $csvDir, 'mtb_product_type', 'mtb_sale_type', true);

            // 在庫
            $this->saveStock($em);

            // 画像の移行はしない
            $em->exec('DELETE FROM dtb_product_image');

            // リレーションエラーになるので
            $em->exec('DELETE FROM dtb_cart');
            $em->exec('DELETE FROM dtb_cart_item');

            // 外部キー制約エラーになるデータを消す
            $em->exec('UPDATE dtb_product_class SET class_category_id1 = NULL WHERE class_category_id1 not in (select id from dtb_class_category)');
            $em->exec('UPDATE dtb_product_class SET class_category_id2 = NULL WHERE class_category_id2 not in (select id from dtb_class_category)');

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 1;');
            } else {
                // シーケンスを進めてあげないといけない
                $this->setIdSeq($em, 'dtb_product');
                $this->setIdSeq($em, 'dtb_product_class');
                $this->setIdSeq($em, 'dtb_class_category');
                $this->setIdSeq($em, 'dtb_class_name');
                $this->setIdSeq($em, 'dtb_category');
                $this->setIdSeq($em, 'dtb_product_stock');
            }

            $em->commit();

            $this->addSuccess('商品データを登録しました。', 'admin');
        } else {
            $this->addDanger('商品データがが見つかりませんでした', 'admin');
        }
    }

    private function saveToP($em, $tmpDir, $csvName, $tableName = null, $allow_zero = false, $i = 1) {

        $tableName = ($tableName)?$tableName:$csvName;
        $em->exec('DELETE FROM ' . $tableName);

        if (file_exists($tmpDir.$csvName.'.csv') == false) {
            // 無視する
            return;

        }
        if (filesize($tmpDir.$csvName.'.csv') == 0) {
            // 無視する
            return;
        }

        if (($handle = fopen($tmpDir.$csvName.'.csv', "r")) !== FALSE) {

            // 文字コード問題が起きる可能性が高いので後で調整が必要になると思う
            $key = fgetcsv($handle);
            $keySize = count($key);

            $columns = $em->getSchemaManager()->listTableColumns($tableName);
            foreach ($columns as $column) {
                $listTableColumns[] =  $column->getName();
            }


            $builder = new BulkInsertQuery($em, $tableName, 20);
            $builder->setColumns($listTableColumns);

            $batchSize = 20;

            while(($row = fgetcsv($handle)) !== FALSE) {
                $value = array();

                // 1行目をkeyとした配列を作る
                $data = array_combine($key, $row);

                // Schemaにあわせた配列を作成する
                foreach($listTableColumns as $column) {
                    if ($column == 'id' && $tableName == 'dtb_product') {
                        $value[$column] = $data['product_id'];

                    } elseif ($column == 'product_status_id') {
                        // 退会が追加された
                        $value[$column] = ($data['del_flg'] == 1) ? '3' : $data['status'];

                    } elseif ($column == 'price02') {
                        $value[$column] = !empty($data[$column])?$data[$column]:0;
                    } elseif ($column == 'name') {
                        $value[$column] = !empty($data[$column])?$data[$column]:'';

                    // カラム名が違うので
                    } elseif ($column == 'description_list') {
                        $value[$column] = isset($data['main_list_comment'])?$data['main_list_comment']:NULL;

                    } elseif ($column == 'description_detail') {
                        $value[$column] = isset($data['main_comment'])?$data['main_comment']:NULL;

                    } elseif ($column == 'search_word') {
                        $value[$column] = isset($data['comment3'])?$data['comment3']:NULL;

                    } elseif ($column == 'free_area') {
                        $value[$column] = $data['sub_title1']."\n".$data['sub_comment1']."\n"
                            .$data['sub_title2']."\n".$data['sub_comment2']."\n"
                            .$data['sub_title3']."\n".$data['sub_comment3']."\n"
                            .$data['sub_title4']."\n".$data['sub_comment4']."\n"
                            .$data['sub_title5']."\n".$data['sub_comment5']."\n"
                            ;

                    // ---> dtb_product_class
                    } elseif ($column == 'sale_type_id') {
                        $value[$column] = isset($data['product_type_id'])?$data['product_type_id']:1;

                    } elseif ($column == 'class_category_id1') {
                        $value[$column] = !empty($data['classcategory_id1'])?$data['classcategory_id1']:NULL;

                        if (!empty($this->dtb_class_combination)) {
                            $value[$column] = $this->dtb_class_combination[$data['class_combination_id']]['classcategory_id1'];
                        }

                    } elseif ($column == 'class_category_id2') {
                        $value[$column] = !empty($data['classcategory_id2'])?$data['classcategory_id2']:NULL;

                        if (!empty($this->dtb_class_combination)) {
                            $value[$column] = $this->dtb_class_combination[$data['class_combination_id']]['classcategory_id2'];
                        }

                    } elseif ($column == 'delivery_fee') {
                        $value[$column] = isset($data['delivery_fee'])?$data['delivery_fee']:NULL;

                    } elseif ($column == 'stock') {
                        $value[$column] = !empty($data['stock'])?$data['stock']:NULL;

                        // dtb_product_stock
                        // todo 2.4系の場合、データが足りない
                        $this->stock[$data['product_class_id']] = $value[$column];

                        // class_category
                    } elseif ($column == 'class_category_id') {
                        $value[$column] = !empty($data['classcategory_id'])?$data['classcategory_id']:0;

                    } elseif ($column == 'class_name_id') {
                        $value[$column] = isset($data['class_id'])?$data['class_id']:NULL;

                    } elseif ($column == 'create_date' || $column == 'update_date') {
                        $value[$column] = (isset($data[$column])&& $data[$column] != '0000-00-00 00:00:00')?$data[$column]:date('Y-m-d H:i:s');
                    } elseif ($column == 'login_date' || $column == 'first_buy_date') {
                        $value[$column] = (!empty($data[$column])&& $data[$column] != '0000-00-00 00:00:00')?$data[$column]:NULL;
                    } elseif ($column == 'creator_id') {
                        $value[$column] = NULL; // 固定

                    } elseif ($column == 'stock_unlimited') {
                        $value[$column] = !empty($data[$column])?$data[$column]:1;
                    } elseif ($column == 'sort_no') {
                        $value[$column] = $data['rank'];
                    } elseif ($column == 'hierarchy') {
                        $value[$column] = $data['level'];

                    } elseif ($column == 'id' && $tableName == 'dtb_product_class') {
                        $value[$column] = $data['product_class_id'];

                    } elseif ($column == 'id' && $tableName == 'dtb_category') {
                        $value[$column] = $data['category_id'];

                    } elseif ($column == 'id' && $tableName == 'dtb_class_category') {
                        $value[$column] = $data['classcategory_id'];

                    } elseif ($column == 'visible' && $tableName == 'dtb_class_category') {
                        $value[$column] = ($data['del_flg'])?0:1;

                    } elseif ($column == 'id' && $tableName == 'dtb_class_name') {
                        $value[$column] = $data['class_id'];

                        // 共通処理
                    } elseif ($column == 'discriminator_type') {
                        $search = array('dtb_','mtb_', '_');
                        $value[$column] = str_replace($search, '', $tableName);

                    } elseif ($allow_zero) {
                        $value[$column] = isset($data[$column])?$data[$column]:NULL;
                    } else {
                        $value[$column] = !empty($data[$column])?$data[$column]:NULL;
                    }

                    // delivery_duration_id
                    if (isset($data['deliv_date_id'])) {
                        // delivery_date_id <-- deliv_date_id (dtb_products)
                        $this->delivery_id[$data['product_id']] = $data['deliv_date_id'];
                    }

                }

                // 別テーブルからのデータなど
                switch ($tableName) {
                    case 'dtb_product_class':
                        $value['delivery_duration_id'] = !empty($this->delivery_id[$value['product_id']])?$this->delivery_id[$value['product_id']]:NULL;

                        // 244用
                        $this->product_class_id[$data['product_id']][$data['classcategory_id1']][$data['classcategory_id2']] = $data['product_class_id'];

                        $value['currency_code'] = 'JPY'; // とりあえず固定

                        // del_flgの代わり
                        if (isset($data['status']) && $data['status']== 1) {
                            $value['visible'] = $data['status']; // todo
                        } else {
                            $value['visible'] = !empty($data['del_flg'])?0:1;
                        }
                        break;
                }

                $builder->setValues($value);

                if (($i % $batchSize) === 0) {
                    $builder->execute();
                }

                $i++;
            }

            if (count($builder->getValues()) > 0) {
                $builder->execute();
            }

            fclose($handle);
            return $i; // indexを返す
        }
    }


    private function fix24baseinfo($em, $tmpDir) {

        if (($handle = fopen($tmpDir.'dtb_baseinfo.csv', "r")) !== FALSE) {

            $key = fgetcsv($handle);
            $keySize = count($key);

            $add_value = array();
            while(($row = fgetcsv($handle)) !== FALSE) {
                // 1行目をkeyとした配列を作る
                $this->baseinfo = array_combine($key, $row);

                $value['tax_rule_id'] = 1;
                $value['calc_rule'] = $this->baseinfo['tax_rule'];
                $value['tax_rate'] = $this->baseinfo['tax'];
                $value['apply_date'] = $value['create_date'] = $value['update_date'] = '1997-04-01 00:00:00';

                $add_value[0] = $value;
            }
            fclose($handle);

            $fpcsv = fopen($tmpDir.'dtb_tax_rule.csv', 'a');
            foreach ($add_value as $row) {
                if ($row === reset($add_value)) {
                    // 最初
                    fputcsv($fpcsv, array_keys($row));
                }
                fputcsv($fpcsv, array_values($row));
            }
            fclose($fpcsv);
        }
    }


    private function fix24Shipping($em, $tmpDir) {

        if (($handle = fopen($tmpDir.'dtb_order.csv', "r")) !== FALSE) {

            $key = fgetcsv($handle);
            $keySize = count($key);

            $i = 1;
            while(($row = fgetcsv($handle)) !== FALSE) {

                // 1行目をkeyとした配列を作る
                $data = array_combine($key, $row);

                $value = array();

                foreach($data as $k => $v) {
                    $value[str_replace('deliv_', 'shipping_', $k)] = $v;
                }

                $value['deliv_time_id'] = $data['deliv_time_id'];
                $value['shipping_id'] = 0;
                $value['rank'] = 0;
                $value['del_flg'] = $data['del_flg'];
                $value['order_id'] = $data['order_id'];
                $value['create_date'] = $data['create_date'];
                $value['update_date'] = $data['update_date'];
                $value['shipping_commit_date'] = $data['commit_date'];

                $add_value[$i] = $value;
                $i++;
            }

            fclose($handle);

            $fpcsv = fopen($tmpDir.'dtb_shipping.csv', 'a');

            foreach ($add_value as $row) {
                if ($row === reset($add_value)) {
                    // 最初
                    fputcsv($fpcsv, array_keys($row));
                }
                fputcsv($fpcsv, array_values($row));
            }
            fclose($fpcsv);
        }
    }


    // 2.4系のclassを追加する
    private function fix24ProductsClass($em, $tmpDir) {

        if (($handle = fopen($tmpDir.'dtb_products_class.csv', "r")) !== FALSE) {

            $key = fgetcsv($handle);
            $keySize = count($key);

            $i = -1;
            while(($row = fgetcsv($handle)) !== FALSE) {

                // 1行目をkeyとした配列を作る
                $data = array_combine($key, $row);
                // 規格がある場合,
                if ($data['classcategory_id1'] != 0 && $data['classcategory_id2'] != 0) {
                    $data['classcategory_id1'] = 0;
                    $data['classcategory_id2'] = 0;
                    $data['status'] = 1;
                    $data['product_class_id'] = $i; // 苦肉の策
                    $add_value[$data['product_id']] = $data;
                    $i--;
                }

            }

            fclose($handle);

            $fpcsv = fopen($tmpDir.'dtb_products_class.csv', 'a');
            foreach ($add_value as $row) {
                fputcsv($fpcsv, array_values($row));
            }
            fclose($fpcsv);
        }
    }

    private function fix211classCombination($em, $platform, $tmpDir) {

        if (($handle = fopen($tmpDir.'dtb_class_combination.csv', "r")) !== FALSE) {

            $key = fgetcsv($handle);
            $keySize = count($key);

            if ($platform == 'mysql') {
                // mysql5.6でエラーになるのでtempは使えない
                $em->exec('
                    CREATE TABLE IF NOT EXISTS dtb_class_combination (
                    class_combination_id int NOT NULL,
                    parent_class_combination_id int,
                    classcategory_id int NOT NULL,
                    level int,
                    PRIMARY KEY(class_combination_id)
                    ) ENGINE=InnoDB;
                ');

            } else {
                $em->exec('
                    CREATE TEMP TABLE dtb_class_combination (
                        class_combination_id int,
                        parent_class_combination_id int,
                        classcategory_id int,
                        level int,
                        PRIMARY KEY (class_combination_id)
                    );
                ');
            }


            $builder = new BulkInsertQuery($em, 'dtb_class_combination', 20);
            $builder->setColumns(array('class_combination_id', 'parent_class_combination_id', 'classcategory_id', 'level'));

            $i = 1;
            $batchSize = 20;
            while(($row = fgetcsv($handle)) !== FALSE) {

                // 1行目をkeyとした配列を作る
                $data = array_combine($key, $row);

                if (!$data['parent_class_combination_id']) {
                    $data['parent_class_combination_id'] = NULL;
                }

                $builder->setValues($data);

                if (($i % $batchSize) === 0) {
                    $builder->execute();
                }
            }
            if (count($builder->getValues()) > 0) {
                $builder->execute();
            }

            fclose($handle);
        }

        $stmt = $em->query("
        SELECT
        class_combination_id
        , (select classcategory_id from dtb_class_combination where class_combination_id = c1.parent_class_combination_id) as classcategory_id1
        , classcategory_id as classcategory_id2
        FROM dtb_class_combination as c1
        where parent_class_combination_id is not null
        ");
        $stmt->execute();
        $all = $stmt->fetchAll();

        $this->dtb_class_combination = array();
        foreach($all as $line) {
            $this->dtb_class_combination[$line['class_combination_id']] = $line;
        }


        $stmt = $em->query("
        SELECT
        class_combination_id
        , classcategory_id as classcategory_id1
        , NULL as classcategory_id2
        FROM dtb_class_combination as c1
        where parent_class_combination_id is null
        ");
        $stmt->execute();
        $all = $stmt->fetchAll();

        foreach($all as $line) {
            $this->dtb_class_combination[$line['class_combination_id']] = $line;
        }
    }

    private function saveStock($em) {
        $tableName = 'dtb_product_stock';
        $columns = $em->getSchemaManager()->listTableColumns($tableName);
        foreach ($columns as $column) {
            $listTableColumns[] =  $column->getName();
        }

        $builder = new BulkInsertQuery($em, $tableName, 20);
        $builder->setColumns($listTableColumns);

        $em->exec('DELETE FROM ' . $tableName);

        $i = 1;
        $batchSize = 20;
        foreach($this->stock as $product_class_id => $stock) {

            $data['id'] = $i;
            $data['product_class_id'] = $product_class_id;
            $data['creator_id'] = NULL; // 固定 ?
            $data['stock'] = $stock;
            $data['create_date'] = $data['update_date'] = date('Y-m-d H:i:s');
            $data['discriminator_type'] = 'productstock';

            $builder->setValues($data);

            // 20件に1回SQLを発行してメモリを開放する。
            if (($i % $batchSize) === 0) {
                $builder->execute();
            }
            $i++;
        }
        if (count($builder->getValues()) > 0) {
            $builder->execute();
            sleep(1);
        }
    }


    // 2.4.4から
    private function cutOff24($tmpDir, $csvName) {

        $tbl_flg = false;
        $col_flg = false;

        if (($handle = fopen($tmpDir.$csvName, "r")) !== FALSE) {

            $fpcsv = '';
            while(($row = fgetcsv($handle)) !== FALSE) {
                //空白行のときはテーブル変更
                if (count($row) <= 1 and $row[0] == "") {
                    $tbl_flg = false;
                    $col_flg = false;
                    $enablePoint = false;
                    $key = array();
                    $i = 1;

                    continue;
                }

                // テーブルフラグがたっていない場合にはテーブル名セット
                if (!$tbl_flg) {
                    // 特定のテーブルのみ
                    switch($row[0]) {
                        case 'dtb_baseinfo':
                        case 'dtb_payment':
                        case 'dtb_deliv':
                        case 'dtb_delivfee':
                        case 'dtb_delivtime':
                        case 'dtb_customer':
                        case 'dtb_products':
                        case 'dtb_products_class':
                        case 'dtb_product_categories':
                        case 'dtb_category':
                        case 'dtb_class':
                        case 'dtb_classcategory':
                        case 'dtb_class_combination':
                        case 'dtb_order':
                        case 'dtb_order_detail':
                            $tableName = $row[0];
                            $allow_zero = false;
                            $tbl_flg = true;

                            $fpcsv = fopen($tmpDir.$tableName.'.csv', 'w');
                            break;
                        case 'dtb_other_deliv':
                            //$tableName = 'dtb_customer_address';
                            $tableName = $row[0];
                            $allow_zero = true;
                            $tbl_flg = true;

                            $fpcsv = fopen($tmpDir.$tableName.'.csv', 'w');
                            break;
                        case 'dtb_index_list': // ゴミデータが交じるので
                            $tbl_flg = true;
                            $tableName = $row[0];
                            $fpcsv = fopen($tmpDir.$tableName.'.csv', 'w');
                            break;

                        case 'dtb_member':
                        case 'mtb_authority':
                        case 'mtb_sex':
                        case 'mtb_job':
                        case 'mtb_product_type':
                            $tableName = $row[0];
                            $allow_zero = true;
                            $tbl_flg = true;
                            $fpcsv = fopen($tmpDir.$tableName.'.csv', 'w');
                            break;
                    }
                    continue;
                }

                if ($tbl_flg) {
                    fputcsv($fpcsv, $row);
                }


            } // end while
            fclose($fpcsv);
            fclose($handle);
        }
    }

    private function setIdSeq($em, $tableName) {
        $max = $em->fetchColumn('SELECT max(id) + 1  FROM ' . $tableName);
        if ($max) {
            $em->exec("SELECT setval('".$tableName."_id_seq', $max);");
        }
    }


    private function saveOrder($em, $csvDir) {

        // 会員系
        if (file_exists($csvDir.'dtb_order.csv') && filesize($csvDir.'dtb_order.csv') > 0 ) {

            $em->beginTransaction();

            $platform = $em->getDatabasePlatform()->getName();

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
                $em->exec("SET SESSION sql_mode = ''"); // STRICT_TRANS_TABLESを無効にする。
            } else {
                $em->exec("SET session_replication_role = replica;"); // need super user
            }

            // todo mtb_order_status.display_order_count

            $this->saveToO($em, $csvDir, 'dtb_order');
            $this->saveToO($em, $csvDir, 'dtb_shipping'); // todo 2.4.4

            $this->saveToO($em, $csvDir, 'dtb_payment');
            $this->saveToO($em, $csvDir, 'dtb_deliv', 'dtb_delivery');
            $this->saveToO($em, $csvDir, 'dtb_delivfee', 'dtb_delivery_fee');
            $this->saveToO($em, $csvDir, 'dtb_delivtime', 'dtb_delivery_time');

            // todo ダウンロード販売の処理
            $this->saveToO($em, $csvDir, 'dtb_order_detail', 'dtb_order_item');
            //$this->saveToO($em, $csvDir, 'dtb_shipment_item', 'dtb_order_item');

            $this->saveToO($em, $csvDir, 'dtb_tax_rule');

            if (!empty($this->order_item)) {
                $this->saveOrderItem($em);
            }

            // 支払いは基本移行しない
            $em->exec('DELETE FROM dtb_payment_option');

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 1;');
            } else {
                $this->setIdSeq($em, 'dtb_order');
                $this->setIdSeq($em, 'dtb_shipping');
                $this->setIdSeq($em, 'dtb_payment');
                $this->setIdSeq($em, 'dtb_delivery');
                $this->setIdSeq($em, 'dtb_delivery_fee');
                $this->setIdSeq($em, 'dtb_delivery_time');
                $this->setIdSeq($em, 'dtb_tax_rule');
            }

            $em->commit();

            $this->addSuccess('受注データを登録しました。', 'admin');
        } else {
            $this->addDanger('受注データが見つかりませんでした', 'admin');
        }
    }

    private function saveToO($em, $tmpDir, $csvName, $tableName = null, $allow_zero = false, $i = 1) {

        $tableName = ($tableName)?$tableName:$csvName;
        $em->exec('DELETE FROM ' . $tableName);

        if (file_exists($tmpDir.$csvName.'.csv') == false) {
            // 無視する
            //$this->addDanger($csvName.'.csv が見つかりませんでした' , 'admin');
            return;
        }
        if (filesize($tmpDir.$csvName.'.csv') == 0) {
            // 無視する
            return;
        }

        if (($handle = fopen($tmpDir.$csvName.'.csv', "r")) !== FALSE) {

            // 文字コード問題が起きる可能性が高いので後で調整が必要になると思う
            $key = fgetcsv($handle);
            $keySize = count($key);

            $columns = $em->getSchemaManager()->listTableColumns($tableName);
            foreach ($columns as $column) {
                $listTableColumns[] =  $column->getName();
            }

            $builder = new BulkInsertQuery($em, $tableName, 20);
            $builder->setColumns($listTableColumns);

            $batchSize = 20;

            while(($row = fgetcsv($handle)) !== FALSE) {
                $value = array();

                // 1行目をkeyとした配列を作る
                $data = array_combine($key, $row);

                // order_ の文字を除去
                foreach($data as $k => $v) {

                    if ($tableName == 'dtb_order') {
                        $data[str_replace('order_', '', $k)] = $v;
                    } else if ($tableName == 'dtb_shipping') {
                        $data[str_replace('shipping_', '', $k)] = $v;
                    }
                }

                // 物理削除になったので
                //if (isset($data['del_flg']) && $data['del_flg'] == 1) continue;

                // Schemaにあわせた配列を作成する
                foreach($listTableColumns as $column) {

                    if ($column == 'id' && $tableName == 'dtb_payment') {
                        $value[$column] = $data['payment_id'];

                    } elseif ($column == 'id' && $tableName == 'dtb_delivery') {
                        $value[$column] = $data['deliv_id'];

                    } elseif ($column == 'id' && $tableName == 'dtb_delivery_fee') {
                        $value[$column] = $i; // todo

                    } elseif ($column == 'id' && $tableName == 'dtb_delivery_time') {
                        $value[$column] = $data['time_id'];


                    } elseif ($column == 'order_status_id') {
                        // 退会が追加された
                        $value[$column] = ($data['del_flg'] == 1) ? '3' : $data['status'];

                    } elseif ($column == 'postal_code') {
                        $value[$column] = mb_substr(mb_convert_kana($data['zip01'].$data['zip02'], 'a'), 0 , 8); //
                        if (empty($value[$column])) { $value[$column] = NULL;}
                    } elseif ($column == 'phone_number') {
                        $value[$column] = mb_substr(mb_convert_kana($data['tel01'].$data['tel02'].$data['tel03'], 'a'), 0 , 14); //14文字制限
                        if (empty($value[$column])) { $value[$column] = NULL;}
                    } elseif ($column == 'sex_id') {
                        $value[$column] = empty($data['sex']) ? NULL : $data['sex'];
                    } elseif ($column == 'job_id') {
                        $value[$column] = empty($data['job']) ? NULL : $data['job']; // 0が入っている場合あり?
                    } elseif ($column == 'pref_id') {
                        $value[$column] = empty($data['pref']) ? NULL : $data['pref'];

                    } elseif ($column == 'delivery_fee_total') {
                        $value[$column] = empty($data['deliv_fee']) ? 0 : $data['deliv_fee'];

                        // --> shipping
                    } elseif ($column == 'delivery_date') {
                        $value[$column] = empty($data['date']) ? NULL : $data['date'];

                    } elseif ($column == 'visible' /*&& $tableName == 'dtb_payment'*/) {
                        $value[$column] = 0;

                        // --> deliv
                    } elseif ($column == 'sale_type_id') {
                        $value[$column] = isset($data['product_type_id'])?$data['product_type_id']:1;
                    } elseif ($column == 'description') {
                        $value[$column] = isset($data['remark'])?$data['remark']:NULL;

                    } elseif ($column == 'delivery_id') {
                        $value[$column] = isset($data['deliv_id'])?$data['deliv_id']:NULL;
                    } elseif ($column == 'delivery_time') {
                        $value[$column] = isset($data['deliv_time'])?$data['deliv_time']:NULL;

                        // --> dtb_order_item
                    } elseif ($column == 'class_category_name1') {
                        $value[$column] = isset($data['classcategory_name1'])?$data['classcategory_name1']:NULL;
                    } elseif ($column == 'class_category_name2') {
                        $value[$column] = isset($data['classcategory_name2'])?$data['classcategory_name2']:NULL;


                    } elseif ($column == 'name01' || $column == 'name02') {
                        $value[$column] = empty($data[$column]) ? 'Not null violation' : $data[$column];
                    } elseif ($column == 'sort_no' && $tableName == 'dtb_shipping') {
                        $value[$column] = $data['id'];

                    } elseif ($column == 'sort_no') {
                        $value[$column] = isset($data['rank'])? $data['rank']:0;

                    } elseif ($column == 'create_date' || $column == 'update_date') {
                        $value[$column] = (isset($data[$column])&& $data[$column] != '0000-00-00 00:00:00')?$data[$column]:date('Y-m-d H:i:s');
                    } elseif ($column == 'payment_date' || $column == 'order_date') {
                        $value[$column] = (!empty($data[$column])&& $data[$column] != '0000-00-00 00:00:00')?$data[$column]:NULL;
                    } elseif ($column == 'creator_id') {
                        $value[$column] = !empty($data[$column])?$data[$column]:1;
                    } elseif ($column == 'charge' || $column == 'use_point' || $column == 'add_point' || $column == 'discount' || $column == 'total' || $column == 'subtotal' || $column == 'tax' || $column == 'payment_total') {
                        $value[$column] = !empty($data[$column])?$data[$column]:0;

                    //} elseif ($column == 'id' && $tableName == 'dtb_customer_address') {
                    //        // カラム名が違うので
                    //    $value[$column] = $i;
                    } elseif ($column == 'discriminator_type') {
                        $search = array('dtb_','mtb_', '_');
                        $value[$column] = str_replace($search, '', $tableName);
                    } elseif ($allow_zero) {
                        $value[$column] = isset($data[$column])?$data[$column]:NULL;
                    } else {
                        $value[$column] = !empty($data[$column])?$data[$column]:NULL;
                    }


                }

                // 別テーブルからのデータなど
                switch ($tableName) {
                    case 'dtb_order':
                        // 配送ID
                        if (isset($data['deliv_id'])) {
                            $this->delivery_id[$data['id']] = $data['deliv_id'];
                        }
                        $value['currency_code'] = 'JPY'; // とりあえず固定

                        if ($data['deliv_fee'] > 0) {
                            $this->order_item[$data['id']]['deliv_fee'] = $data['deliv_fee'];
                        }
                        if ($data['charge'] > 0) {
                            $this->order_item[$data['id']]['charge'] = $data['charge'];
                        }
                        if ($data['discount'] > 0) {
                            $this->order_item[$data['id']]['discount'] = $data['discount'];
                        }
                        break;

                    case 'dtb_shipping':
                        $value['id'] = $i;
                        $this->shipping_id[$data['order_id']][$data['shipping_id']] = $i;

                        $value['delivery_id'] = !empty($this->delivery_id[$value['order_id']])?$this->delivery_id[$value['order_id']]:NULL;
                        $value['delivery_time'] = empty($data['time']) ? NULL : $data['time'];
                        break;

                    case 'dtb_tax_rule':
                        $value['id'] = $data['tax_rule_id'];
                        $value['tax_adjust'] = 0;
                        $value['rounding_type_id'] = $data['calc_rule'];
                        break;

                    case 'dtb_order_item':
                        if (isset($data['order_detail_id'])) {
                            $value['id'] = $data['order_detail_id'];
                        } else {
                            $value['id'] = $i; // 2.4.4
                        }
                        $value['rounding_type_id'] = 1;
                        $value['tax_type_id'] = 1;
                        $value['tax_display_type_id'] = 1;

                        $value['tax_rule_id'] = isset($data['tax_rule'])?$data['tax_rule']:1;

                        // 2.4.4
                        if (isset($this->baseinfo['tax'])) {
                            $value['tax_rate'] = $data['tax_rate'] = $this->baseinfo['tax'];
                            $data['point_rate'] = $this->baseinfo['point_rate'];
                            $value['product_class_id'] = $this->product_class_id[$data['product_id']][$data['classcategory_id1']][$data['classcategory_id2']];
                        }

                        if (!empty($data['price']) && !empty($data['tax_rate']) && !empty($data['quantity'])) {
                            $value['tax'] = round($data['price'] * $data['tax_rate']/100) * $data['quantity'];
                        }

                        $value['order_item_type_id'] = 1; // 商品で固定する
                        $value['currency_code'] = 'JPY'; // とりあえず固定

                        if (isset($this->shipping_id[$data['order_id']][0])) {
                            $value['shipping_id'] = $this->shipping_id[$data['order_id']][0];
                        } else {
                            $value['shipping_id'] = NULL; // ダウンロード販売
                        }
                        break;
                }

                $builder->setValues($value);

                if (($i % $batchSize) === 0) {
                    $builder->execute();
                }

                $i++;
            }

            if (count($builder->getValues()) > 0) {
                $builder->execute();
            }

            fclose($handle);
            return $i; // indexを返す
        }
    }


    private function saveOrderItem($em) {
        $tableName = 'dtb_order_item';
        $columns = $em->getSchemaManager()->listTableColumns($tableName);
        foreach ($columns as $column) {
            $listTableColumns[] =  $column->getName();
            $data[$column->getName()] = NULL;
        }

        $builder = new BulkInsertQuery($em, $tableName, 20);
        $builder->setColumns($listTableColumns);

        //
        $i = $em->fetchColumn('SELECT max(id) + 1  FROM ' . $tableName);
        $batchSize = 20;
        foreach($this->order_item as $order_id => $type) {

            foreach($type as $key => $value) {

                switch ($key) {
                case 'deliv_fee':
                    $data['order_item_type_id'] = 2;
                    $data['product_name'] = '送料';
                    break;
                case 'charge':
                    $data['order_item_type_id'] = 3;
                    $data['product_name'] = '手数料';
                    break;
                case 'discount':
                    $data['order_item_type_id'] = 4;
                    $data['product_name'] = '割引';
                    break;
                }
                $data['price'] = $value;
                $data['tax'] = 0;
                $data['tax_rate'] = 0;
                $data['quantity'] = 1;
                $data['id'] = $i;
                $data['shipping_id'] = $this->shipping_id[$order_id][0];
                $data['order_id'] = $order_id;
                $data['tax_type_id'] = 1;
                $data['rounding_type_id'] = 1;
                $data['tax_display_type_id'] = 2;
                $data['discriminator_type'] = 'orderitem';

                $builder->setValues($data);
                // 20件に1回SQLを発行してメモリを開放する。
                if (($i % $batchSize) === 0) {
                    $builder->execute();
                }
                $i++;
            }
        }
        if (count($builder->getValues()) > 0) {
            $builder->execute();
            sleep(1);
        }
    }
}
