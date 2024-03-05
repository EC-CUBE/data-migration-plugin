<?php

namespace Plugin\DataMigration42\Controller\Admin;

//use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Connection;

use Eccube\Controller\AbstractController;
use Eccube\Service\PluginService;
use Eccube\Util\StringUtil;
use nobuhiko\BulkInsertQuery\BulkInsertQuery;
use Plugin\DataMigration42\Form\Type\Admin\ConfigType;
use Symfony\Component\Routing\Annotation\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Template;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\HttpFoundation\Request;
use wapmorgan\UnifiedArchive\UnifiedArchive;

class ConfigController extends AbstractController
{
    /** @var pluginService */
    protected $pluginService;

    /** @var array */
    protected $tax_rule = [];

    /** @var array */
    protected $delivery_time = [];

    /** @var Connection */
    protected $em;
    /** @var bool */
    protected $flag_244 = false;
    /** @var bool */
    protected $flag_3 = false;
    /** @var bool */
    protected $flag_4 = false;
    /** @var array */
    protected $delivery_id = [];
    /** @var array */
    protected $stock = [];
    /** @var array */
    protected $shipping_id = [];
    /** @var array */
    protected $product_class_id = [];
    /** @var array */
    protected $order_item = [];
    /** @var array */
    protected $product_images = [];
    /** @var array */
    protected $baseinfo = [];
    /** @var array */
    protected $dtb_class_combination = [];
    /** @var array */
    protected $shipping_order = [];

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
     * @Route("/%eccube_admin_route%/data_migration42/config", name="data_migration42_admin_config")
     * @Template("@DataMigration42/admin/config.twig")
     */
    public function index(Request $request, Connection $em)
    {
        $this->delivery_id = [];
        $this->stock = [];
        $this->shipping_id = [];
        $this->product_class_id = [];
        $this->order_item = [];
        $this->product_images = [];

        $form = $this->createForm(ConfigType::class);
        $form->handleRequest($request);

        if (0 === strpos(PHP_OS, 'WIN')) {
            setlocale(LC_CTYPE, 'C');
        }

        if ($form->isSubmitted() && $form->isValid()) {
            // logをオフにしてメモリを減らす
            $em->getConfiguration()->setSQLLogger(null);

            $formFile = $form['import_file']->getData();

            $tmpFile = $formFile->getClientOriginalName();
            $tmpDir = $this->pluginService->createTempDir();
            $formFile->move($tmpDir, $tmpFile);

            $archive = UnifiedArchive::open($tmpDir.'/'.$tmpFile);
            $fileNames = $archive->getFileNames();
            // 解凍
            $archive->extractFiles($tmpDir, $fileNames);

            $this->flag_244 = false;
            //
            $this->em = $em;

            // 圧縮方式の間違いに対応する
            $path = pathinfo($fileNames[0]);

            if ($path != '.') {
                $csvDir = $tmpDir.'/'.$path['dirname'].'/';
            } else {
                $csvDir = $tmpDir.'/';
            }

            // 2.4.4系の場合の処理
            if ($archive->isFileExists($path['dirname'].'/bkup_data.csv')) {

                //$csvDir = $tmpDir.'/'.$fileNames[0];
                $this->cutOff24($csvDir, 'bkup_data.csv');

                // 2.4.4系の場合の処理
                if (file_exists($csvDir.'dtb_products_class.csv')) {
                    // 2.11の場合は通さない
                    if (!file_exists($csvDir.'dtb_class_combination.csv')) {
                        $this->flag_244 = true;
                        // create dtb_shipping
                        $this->fix24Shipping($em, $csvDir);
                        $this->fix24ProductsClass($em, $csvDir);
                    }
                }
            }

            // 2.13以外全部
            if (!file_exists($csvDir.'dtb_tax_rule.csv')) {
                // 税率など
                $this->fix24baseinfo($em, $csvDir);
            }

            $this->flag_4 = false;
            // 4.0/4.1系の場合
            if (file_exists($csvDir.'dtb_order_item.csv')) {
                $this->flag_4 = true;
            }

            $this->flag_3 = false;
            if ($this->flag_4 == false) {
                // 3系の場合
                if (file_exists($csvDir.'dtb_product.csv')) {
                    $this->flag_3 = true;
                }
            }

            // 会員・受注のみ移行
            if ($form['customer_order_only']->getData()) {
                $this->saveCustomerAndOrder($em, $csvDir);
            // 全データ移行
            } else {
                $this->saveCustomer($em, $csvDir);
                $this->saveProduct($em, $csvDir);
                $this->saveOrder($em, $csvDir);
            }

            // 削除
            $fs = new Filesystem();
            $fs->remove($tmpDir);

            return $this->redirectToRoute('data_migration42_admin_config');
        }

        return [
            'form' => $form->createView(),
            'max_upload_size' => self::checkUploadSize(),
        ];
    }

    private function saveCustomerAndOrder($em, $csvDir)
    {
        $em->beginTransaction();
        $platform = $em->getDatabasePlatform()->getName();

        if ($platform == 'mysql') {
            $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
            $em->exec("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"); // STRICT_TRANS_TABLESを無効にする。
        } else {
            $em->exec('SET session_replication_role = replica;'); // need super user
        }

        // 会員
        $this->saveToC($em, $csvDir, 'dtb_customer');

        if ($this->flag_4) {
            $this->saveToC($em, $csvDir, 'dtb_customer_address');
            $this->saveToO($em, $csvDir, 'dtb_delivery_time');
        } else if ($this->flag_3) {
            $this->saveToC($em, $csvDir, 'dtb_customer_address');
            $this->saveToO($em, $csvDir, 'dtb_delivery_time');
        } else {
            $this->saveToC($em, $csvDir, 'dtb_other_deliv', 'dtb_customer_address', false, 1/*$index*/);
        }

        // 受注
        $this->saveToO($em, $csvDir, 'dtb_order');
        $this->saveToO($em, $csvDir, 'dtb_shipping');
        $this->saveToO($em, $csvDir, 'dtb_mail_history', 'dtb_mail_history');
        if ($this->flag_4) {
            $this->saveToO($em, $csvDir, 'dtb_order_item');
        } else {
            $this->saveToO($em, $csvDir, 'dtb_order_detail', 'dtb_order_item', true);
        }

        if (!empty($this->order_item)) {
            // すでに移行されている税率設定から取得する
            $sql = 'SELECT * FROM dtb_tax_rule WHERE product_id IS NULL AND product_class_id IS NULL ORDER BY apply_date DESC';
            $stmt = $em->query($sql);
            $tax_rules = $stmt->fetchAllAssociative();
            foreach ($tax_rules as $tax_rule) {
                $this->tax_rule[$tax_rule['apply_date']] = [
                    'rounding_type_id' => $tax_rule['rounding_type_id'],
                    'tax_rate' => $tax_rule['tax_rate'],
                    'apply_date' => $tax_rule['apply_date'],
                ];
            }
            $this->saveOrderItem($em);
        }

        if ($platform == 'mysql') {
            $em->exec('SET FOREIGN_KEY_CHECKS = 1;');
        } else {
            $this->setIdSeq($em, 'dtb_customer');
            $this->setIdSeq($em, 'dtb_customer_address');
            $this->setIdSeq($em, 'dtb_order');
            $this->setIdSeq($em, 'dtb_order_item');
            $this->setIdSeq($em, 'dtb_shipping');
            $this->setIdSeq($em, 'dtb_mail_history');
        }

        $em->commit();
        $this->addSuccess('会員データ・受注データを登録しました。', 'admin');
    }

    private function saveCustomer($em, $csvDir)
    {
        // 会員系
        if (file_exists($csvDir.'dtb_customer.csv') && filesize($csvDir.'dtb_customer.csv') > 0) {
            $em->beginTransaction();

            $platform = $em->getDatabasePlatform()->getName();

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
                $em->exec("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"); // STRICT_TRANS_TABLESを無効にする。
            } else {
                $em->exec('SET session_replication_role = replica;'); // need super user
            }

            $this->saveToC($em, $csvDir, 'mtb_job', null, true);
            $this->saveToC($em, $csvDir, 'mtb_sex', null, true);

            if ($this->flag_4) {
                $this->saveToC($em, $csvDir, 'mtb_customer_order_status', null, true);
                $this->saveToC($em, $csvDir, 'mtb_customer_status', null, true);
            }

            $this->saveToC($em, $csvDir, 'dtb_customer');
            if ($this->flag_4) {
                $this->saveToC($em, $csvDir, 'dtb_customer_address');
            } else if ($this->flag_3) {
                // fixme 余計なデータが移行される
                $this->saveToC($em, $csvDir, 'dtb_customer_address');
            } else {
                $this->saveToC($em, $csvDir, 'dtb_other_deliv', 'dtb_customer_address', false, 1);
            }

            $this->saveToC($em, $csvDir, 'mtb_authority', null, true);
            $this->saveToC($em, $csvDir, 'dtb_member', null, true);

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

    private function saveToC($em, $tmpDir, $csvName, $tableName = null, $allow_zero = false, $i = 1)
    {
        $tableName = ($tableName) ? $tableName : $csvName;
        $this->resetTable($em, $tableName);

        if (file_exists($tmpDir.$csvName.'.csv') == false) {
            // 無視する
            //$this->addDanger($csvName.'.csv が見つかりませんでした' , 'admin');
            return;
        }
        if (filesize($tmpDir.$csvName.'.csv') == 0) {
            // 無視する
            return;
        }

        if (($handle = fopen($tmpDir.$csvName.'.csv', 'r')) !== false) {
            // 文字コード問題が起きる可能性が高いので後で調整が必要になると思う
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));

            $keySize = count($key);
            $columns = $em->getSchemaManager()->listTableColumns($tableName);

            $listTableColumns = [];
            foreach ($columns as $column) {
                $columnName = $column->getName();
                if ($tableName === 'dtb_member') {
                    if ($columnName === 'two_factor_auth_key' || $columnName === 'two_factor_auth_enabled') {
                        continue;
                    }
                }
                $listTableColumns[] = $columnName;
            }

            $builder = new BulkInsertQuery($em, $tableName);
            $builder->setColumns($listTableColumns);

            $batchSize = 20;

            while (($row = fgetcsv($handle)) !== false) {
                $value = [];

                // 1行目をkeyとした配列を作る
                $data = $this->convertNULL(array_combine($key, $row));

                // Schemaにあわせた配列を作成する
                foreach ($listTableColumns as $column) {
                    if ($this->flag_4 == true) {
                        if ($column == 'buy_times') {
                            $value[$column] = isset($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'creator_id') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 1;
                        } elseif ($column == 'create_date' || $column == 'update_date') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : date('Y:m:d H:i:s');
                        } elseif ($column == 'login_date' || $column == 'first_buy_date') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        } elseif ($column == 'point') {
                            $value[$column] = empty($data[$column]) ? 0 : (int) $data[$column];
                        } elseif ($allow_zero) {
                            $value[$column] = isset($data[$column]) ? $data[$column] : null;
                        } else {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        }
                    } else {
                        if ($column == 'id' && $tableName == 'dtb_customer') { // fixme
                            $value[$column] = $data['customer_id'];
                        } elseif ($column == 'customer_status_id') {
                            // 退会が追加された
                            $value[$column] = ($data['del_flg'] == 1) ? '3' : $data['status'];
                        } elseif ($column == 'postal_code') {
                            $value[$column] = mb_substr(mb_convert_kana($data['zip01'].$data['zip02'], 'a'), 0, 8);
                            if (empty($value[$column])) {
                                $value[$column] = null;
                            }
                        } elseif ($column == 'phone_number') {
                            $value[$column] = mb_substr(mb_convert_kana($data['tel01'].$data['tel02'].$data['tel03'], 'a'), 0, 14); //14文字制限
                            if (empty($value[$column])) {
                                $value[$column] = null;
                            }
                        } elseif ($column == 'sex_id') {
                            $value[$column] = empty($data['sex']) ? null : $data['sex'];
                        } elseif ($column == 'job_id') {
                            $value[$column] = empty($data['job']) ? null : $data['job']; // 0が入っている場合あり?
                        } elseif ($column == 'pref_id') {
                            $value[$column] = empty($data['pref']) ? null : $data['pref'];
                        } elseif ($column == 'work_id') {
                            // 削除されているメンバーは非稼働で登録
                            $value[$column] = ($data['del_flg'] == 1) ? 0 : $data['work'];
                        } elseif ($column == 'authority_id') {
                            $value[$column] = $data['authority'];
                        } elseif ($column == 'email') {
                            // 退会時はランダムな値に更新
                            if ($data['del_flg'] == 1) {
                                $value[$column] = StringUtil::random(60).'@dummy.dummy';
                            } else {
                                $value[$column] = empty($data[$column]) ? 'Not null violation' : $data[$column];
                            }
                        } elseif ($column == 'password' || $column == 'name01' || $column == 'name02') {
                            $value[$column] = empty($data[$column]) ? 'Not null violation' : $data[$column];
                        } elseif ($column == 'sort_no') {
                            if ($this->flag_4 == true) {
                                $value[$column] = $data['sort_no'];
                            } else {
                                $value[$column] = $data['rank'];
                            }
                        } elseif ($column == 'create_date' || $column == 'update_date') {
                            $value[$column] = (isset($data[$column]) && $data[$column] != '0000-00-00 00:00:00') ? self::convertTz($data[$column]) : date('Y-m-d H:i:s');
                        } elseif ($column == 'login_date' || $column == 'first_buy_date') {
                            $value[$column] = (!empty($data[$column]) && $data[$column] != '0000-00-00 00:00:00') ? self::convertTz($data[$column]) : null;
                        } elseif ($column == 'secret_key') { // 実験
                            $value[$column] = mt_rand();
                        } elseif ($column == 'point') {
                            $value[$column] = empty($data[$column]) ? 0 : (int) $data[$column];
                        } elseif ($column == 'salt') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;  // @see https://github.com/EC-CUBE/data-migration-plugin/issues/38
                        } elseif ($column == 'creator_id') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 1;
                        } elseif ($column == 'plg_mailmagazine_flg') {
                            $value[$column] = (!empty($data['mailmaga_flg']) && $data['mailmaga_flg'] != 3) ? 1 : 0; // メルマガプラグイン
                        } elseif ($column == 'id' && $tableName == 'dtb_member') {
                            $value[$column] = $data['member_id'];
                        } elseif ($column == 'id' && $tableName == 'dtb_customer_address') {
                            // カラム名が違うので
                            $value[$column] = $i;
                        } elseif ($column == 'discriminator_type') {
                            $search = ['dtb_', 'mtb_', '_'];
                            $value[$column] = str_replace($search, '', $tableName);
                        } elseif ($allow_zero) {
                            $value[$column] = isset($data[$column]) ? $data[$column] : null;
                        } else {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        }
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

    private function saveProduct($em, $csvDir)
    {
        if ($this->flag_4) {
            $product_db_name = 'dtb_product';
        } else if ($this->flag_3) {
            $product_db_name = 'dtb_product';
        } else {
            $product_db_name = 'dtb_products';
        }

        if (file_exists($csvDir.$product_db_name.'.csv') && filesize($csvDir.$product_db_name.'.csv') > 0) {
            $em->beginTransaction();

            $platform = $em->getDatabasePlatform()->getName();

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
                $em->exec("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"); // STRICT_TRANS_TABLESを無効にする。
            } else {
                $em->exec('SET session_replication_role = replica;');
            }

            // 2.11系の処理
            if (file_exists($csvDir.'dtb_class_combination.csv')) {
                $this->fix211classCombination($em, $platform, $csvDir);
            }

            if ($this->flag_4) {
                $this->saveToC($em, $csvDir, 'mtb_product_status', null, true);
                $this->saveToC($em, $csvDir, 'mtb_sale_type', null, true);
                $this->saveToP($em, $csvDir, 'dtb_product');
                $this->saveToO($em, $csvDir, 'dtb_delivery_duration', null, true);
                $this->saveToP($em, $csvDir, 'dtb_product_class');
                $this->saveToP($em, $csvDir, 'dtb_class_category');
                $this->saveToP($em, $csvDir, 'dtb_class_name');
                $this->saveToP($em, $csvDir, 'dtb_product_category');
                $this->saveToP($em, $csvDir, 'dtb_product_stock');
                $this->saveToP($em, $csvDir, 'dtb_product_image');
                $this->saveToP($em, $csvDir, 'dtb_tag');
                $this->saveToP($em, $csvDir, 'dtb_product_tag');
                $this->saveToP($em, $csvDir, 'dtb_customer_favorite_product');
            } else if ($this->flag_3) {
                $this->saveToP($em, $csvDir, 'dtb_product');
                $this->saveToP($em, $csvDir, 'dtb_product_class');
                $this->saveToP($em, $csvDir, 'dtb_class_category');
                $this->saveToP($em, $csvDir, 'dtb_class_name');
                $this->saveToP($em, $csvDir, 'dtb_product_category');
                $this->saveToP($em, $csvDir, 'dtb_product_stock');
                $this->saveToP($em, $csvDir, 'dtb_product_image');
                $this->saveToP($em, $csvDir, 'dtb_product_tag');
                $this->saveToP($em, $csvDir, 'mtb_tag', 'dtb_tag');
                $this->saveToP($em, $csvDir, 'dtb_customer_favorite_product');

            } else {
                $this->saveToP($em, $csvDir, 'dtb_products', 'dtb_product');
                $this->saveToP($em, $csvDir, 'dtb_products_class', 'dtb_product_class');
                $this->saveToP($em, $csvDir, 'dtb_classcategory', 'dtb_class_category');
                $this->saveToP($em, $csvDir, 'dtb_class', 'dtb_class_name');
                $this->saveToP($em, $csvDir, 'dtb_product_categories', 'dtb_product_category');
                $this->saveToP($em, $csvDir, 'dtb_product_status', 'dtb_product_tag');
                $this->saveToP($em, $csvDir, 'mtb_status', 'dtb_tag');

                $this->saveToP($em, $csvDir, 'dtb_customer_favorite_products', 'dtb_customer_favorite_product');

                // 在庫
                $this->saveStock($em);
                // 画像
                $this->saveProductImage($em);
            }

            $this->saveToP($em, $csvDir, 'dtb_category');
            if (file_exists($csvDir.'mtb_product_type.csv')) {
                $this->saveToP($em, $csvDir, 'mtb_product_type', 'mtb_sale_type', true);
            }

            // 削除済み商品を4系のデータ構造に合わせる
            $this->fixDeletedProduct($em);

            // リレーションエラーになるので
            $em->exec('DELETE FROM dtb_cart');
            $em->exec('DELETE FROM dtb_cart_item');

            // 外部キー制約エラーになるデータを消す
            $em->exec('DELETE FROM dtb_class_category WHERE id = 0');
            $em->exec('UPDATE dtb_product_class SET class_category_id1 = NULL WHERE class_category_id1 not in (select id from dtb_class_category)');
            $em->exec('UPDATE dtb_product_class SET class_category_id2 = NULL WHERE class_category_id2 not in (select id from dtb_class_category)');

            $em->exec('delete from dtb_product_tag where id in (
                select id from (select t1.id from dtb_product_tag t1 left join dtb_tag t2 on t1.tag_id = t2.id where t2.id is null) as tmp
            );');
            $em->exec('delete from dtb_product_tag where id in (
                select id from (select t1.id from dtb_product_tag t1 left join dtb_product t2 on t1.product_id = t2.id where t2.id is null) as tmp
            );');

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
                $this->setIdSeq($em, 'dtb_product_image');
                $this->setIdSeq($em, 'dtb_product_tag');
                $this->setIdSeq($em, 'dtb_tag');
                $this->setIdSeq($em, 'dtb_customer_favorite_product');
            }

            $em->commit();

            $this->addSuccess('商品データを登録しました。', 'admin');
        } else {
            $this->addDanger('商品データがが見つかりませんでした', 'admin');
        }
    }

    private function saveToP($em, $tmpDir, $csvName, $tableName = null, $allow_zero = false, $i = 1)
    {
        $tableName = ($tableName) ? $tableName : $csvName;
        $this->resetTable($em, $tableName);

        if (file_exists($tmpDir.$csvName.'.csv') == false) {
            // 無視する
            return;
        }
        if (filesize($tmpDir.$csvName.'.csv') == 0) {
            // 無視する
            return;
        }

        if (($handle = fopen($tmpDir.$csvName.'.csv', 'r')) !== false) {
            // 文字コード問題が起きる可能性が高いので後で調整が必要になると思う
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));
            $keySize = count($key);

            $columns = $em->getSchemaManager()->listTableColumns($tableName);
            $listTableColumns = [];
            foreach ($columns as $column) {
                $listTableColumns[] = $column->getName();
            }

            $builder = new BulkInsertQuery($em, $tableName);
            $builder->setColumns($listTableColumns);

            $batchSize = 20;

            while (($row = fgetcsv($handle)) !== false) {
                $value = [];

                // 1行目をkeyとした配列を作る
                $data = $this->convertNULL(array_combine($key, $row));

                if ($this->flag_3) {
                    if (isset($data['class_category_id1'])) {
                        $data['classcategory_id1'] = $data['class_category_id1'];
                    }
                    if (isset($data['class_category_id2'])) {
                        $data['classcategory_id2'] = $data['class_category_id2'];
                    }
                    if (isset($data['class_category_id'])) {
                        $data['classcategory_id'] = $data['class_category_id'];
                    }
                    if (isset($data['class_name_id'])) {
                        $data['class_id'] = $data['class_name_id'];
                    }
                    if (isset($data['description_detail'])) {
                        $data['main_comment'] = $data['description_detail'];
                    }
                    if (isset($data['search_word'])) {
                        $data['comment3'] = $data['search_word'];
                    }
                }

                // Schemaにあわせた配列を作成する
                foreach ($listTableColumns as $column) {
                    if ($this->flag_4 == true) {
                        if ($column == 'class_category_id1') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        } elseif ($column == 'class_category_id2') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        } elseif ($column == 'stock_unlimited') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'sort_no') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'creator_id') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 1;
                        } elseif ($column == 'create_date' || $column == 'update_date') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : date('Y:m:d H:i:s');
                        } elseif ($column == 'display_order_count') {
                            $value[$column] = empty($data[$column]) ? 0 : $data[$column];
                        } elseif ($column == 'visible') {
                            $value[$column] = empty($data[$column]) ? 0 : $data[$column];
                        } elseif ($allow_zero) {
                            $value[$column] = isset($data[$column]) ? $data[$column] : null;
                        } else {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        }
                    } else {
                       if ($column == 'id' && $tableName == 'dtb_product') {
                            $value[$column] = $data['product_id'];

                        } elseif ($column == 'id' && $tableName == 'dtb_customer_favorite_product') {
                            $value[$column] = $i;

                        } elseif ($column == 'product_status_id') {
                            // 退会が追加された
                            $value[$column] = ($data['del_flg'] == 1) ? '3' : $data['status'];
                        } elseif ($column == 'price02') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'name') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : '';

                        // カラム名が違うので
                        } elseif ($column == 'description_list') {
                            $value[$column] = isset($data['main_list_comment'])
                                ? mb_substr($data['main_list_comment'], 0, 3999)
                                : null;
                        } elseif ($column == 'description_detail') {
                            $value[$column] = isset($data['main_comment'])
                                ? mb_substr($data['main_comment'], 0, 3999)
                                : null;
                        } elseif ($column == 'search_word') {
                            $value[$column] = isset($data['comment3'])
                                ? mb_substr($data['comment3'], 0, 3999)
                                : null;
                        } elseif ($column == 'free_area' && isset($data['sub_title1'])) {
                            $value[$column] = $data['sub_title1']."\n".$data['sub_comment1']."\n"
                                .$data['sub_title2']."\n".$data['sub_comment2']."\n"
                                .$data['sub_title3']."\n".$data['sub_comment3']."\n"
                                .$data['sub_title4']."\n".$data['sub_comment4']."\n"
                                .$data['sub_title5']."\n".$data['sub_comment5']."\n"
                                ;

                        // ---> dtb_product_class
                        } elseif ($column == 'sale_type_id') {
                            $value[$column] = isset($data['product_type_id']) ? $data['product_type_id'] : 1;
                        } elseif ($column == 'class_category_id1') {
                            $value[$column] = !empty($data['classcategory_id1']) ? $data['classcategory_id1'] : null;

                            if (!empty($this->dtb_class_combination) && !empty($data['class_combination_id'])) {
                                $value[$column] = $this->dtb_class_combination[$data['class_combination_id']]['classcategory_id1'];
                            }
                        } elseif ($column == 'class_category_id2') {
                            $value[$column] = !empty($data['classcategory_id2']) ? $data['classcategory_id2'] : null;

                            if (!empty($this->dtb_class_combination) && !empty($data['class_combination_id'])) {
                                $value[$column] = $this->dtb_class_combination[$data['class_combination_id']]['classcategory_id2'];
                            }
                        } elseif ($column == 'delivery_fee') {
                            $value[$column] = (isset($data['delivery_fee']) && is_numeric($data['delivery_fee'])) ? $data['delivery_fee'] : null;
                        } elseif ($column == 'stock') {
                            $value[$column] = isset($data['stock']) && $data['stock'] !== ''
                                ? $data['stock']
                                : null;

                            // dtb_product_stock
                            // todo 2.4系の場合、データが足りない
                            $this->stock[$data['product_class_id']] = $value[$column];

                        // class_category
                        } elseif ($column == 'class_category_id') {
                            $value[$column] = !empty($data['classcategory_id']) ? $data['classcategory_id'] : 0;
                        } elseif ($column == 'class_name_id') {
                            $value[$column] = isset($data['class_id']) ? $data['class_id'] : null;
                        } elseif ($column == 'create_date' || $column == 'update_date') {
                            $value[$column] = (isset($data[$column]) && strpos($data[$column], '000') === false) ? self::convertTz($data[$column]) : date('Y-m-d H:i:s');
                        } elseif ($column == 'login_date' || $column == 'first_buy_date') {
                            $value[$column] = (!empty($data[$column]) && $data[$column] != '0000-00-00 00:00:00') ? self::convertTz($data[$column]) : null;
                        } elseif ($column == 'creator_id') {
                            $value[$column] = null; // 固定
                        } elseif ($column == 'stock_unlimited') {
                            $value[$column] = empty($data[$column]) ? 0 : 1;
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
                            $value[$column] = ($data['del_flg']) ? 0 : 1;
                        } elseif ($column == 'id' && $tableName == 'dtb_class_name') {
                            $value[$column] = $data['class_id'];
                        } elseif ($column == 'id' && $tableName == 'dtb_product_stock') {
                            $value[$column] = $data['product_stock_id'];
                        } elseif ($column == 'id' && $tableName == 'dtb_product_image') {
                            $value[$column] = $data['product_image_id'];
                        } elseif ($column == 'id' && $tableName == 'dtb_product_tag') {
                            $value[$column] = $i;
                        } elseif ($column == 'tag_id' && $tableName == 'dtb_product_tag') {
                            if ($this->flag_3) {
                                $value[$column] = isset($data['tag']) && strlen($data['tag'] > 0) ? $data['tag'] : 0;
                            } else {
                                $value[$column] = isset($data['product_status_id']) && strlen($data['product_status_id'] > 0) ? $data['product_status_id'] : 0;
                            }
                            // 共通処理
                        } elseif ($column == 'discriminator_type') {
                            $search = ['dtb_', 'mtb_', '_'];
                            $value[$column] = str_replace($search, '', $tableName);
                        } elseif ($allow_zero) {
                            $value[$column] = isset($data[$column]) ? $data[$column] : null;
                        } else {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        }

                        // delivery_duration_id
                        if (isset($data['deliv_date_id'])) {
                            // delivery_date_id <-- deliv_date_id (dtb_products)
                            $this->delivery_id[$data['product_id']] = $data['deliv_date_id'];
                        }

                        // product_image
                        if (!empty($data['main_large_image'])) {
                            $this->product_images[$data['product_id']] = [$data['main_large_image']];
                        } elseif (!empty($data['main_image'])) {
                            $this->product_images[$data['product_id']] = [$data['main_image']];
                        } elseif (!empty($data['main_list_image'])) {
                            $this->product_images[$data['product_id']] = [$data['main_list_image']];
                        }
                        for ($sub_image_id=1; $sub_image_id <= 6; $sub_image_id++) {
                            if (!empty($data['sub_large_image' . $sub_image_id])) {
                                $this->product_images[$data['product_id']][] = $data['sub_large_image' . $sub_image_id];
                            } elseif (!empty($data['sub_image' . $sub_image_id])) {
                                $this->product_images[$data['product_id']][] = $data['sub_image' . $sub_image_id];
                            }
                        }
                    }
                }

                // 別テーブルからのデータなど
                switch ($tableName) {
                    case 'dtb_product_class':
                        if ($this->flag_4 == false) {
                            $value['delivery_duration_id'] = !empty($this->delivery_id[$value['product_id']]) ? $this->delivery_id[$value['product_id']] : null;

                            // 244用
                            if ($this->flag_244) {
                                $this->product_class_id[$data['product_id']][$data['classcategory_id1']][$data['classcategory_id2']] = $data['product_class_id'];
                            }

                            $value['currency_code'] = 'JPY'; // とりあえず固定

                            // del_flgの代わり
                            if (isset($data['status']) && $data['status'] == 1) {
                                $value['visible'] = $data['status']; // todo
                            } else {
                                $value['visible'] = !empty($data['del_flg']) ? 0 : 1;
                            }
                        } else {
                            $value['visible'] = empty($data['visible']) ? 0 : (int) $data['visible'];
                        }
                        break;
                    case 'dtb_customer_favorite_product':

                        if ($this->flag_4 == false) {
                            // 3系には del_flg がある
                            if ($data['del_flg'] == 1) {
                                unset($value);
                                continue 2;
                            }
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

    private function fix24baseinfo($em, $tmpDir)
    {
        if (!file_exists($tmpDir.'dtb_baseinfo.csv')) {
            return;
        }

        if (($handle = fopen($tmpDir.'dtb_baseinfo.csv', 'r')) !== false) {
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));
            $keySize = count($key);

            $add_value = [];
            while (($row = fgetcsv($handle)) !== false) {
                // 1行目をkeyとした配列を作る
                $this->baseinfo = $this->convertNULL(array_combine($key, $row));

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

    private function fix24Shipping($em, $tmpDir)
    {
        if (($handle = fopen($tmpDir.'dtb_order.csv', 'r')) !== false) {
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));
            $keySize = count($key);

            $i = 1;
            $add_value = [];
            while (($row = fgetcsv($handle)) !== false) {
                // 1行目をkeyとした配列を作る
                $data = $this->convertNULL(array_combine($key, $row));

                $value = [];

                foreach ($data as $k => $v) {
                    $value[str_replace('deliv_', 'shipping_', $k)] = $v;
                }

                $value['deliv_time_id'] = isset($data['deliv_time_id']) ? $data['deliv_time_id'] : null;
                $value['shipping_id'] = 0;
                $value['rank'] = 0;
                if (!empty($value['shipping_date'])) {
                    // 変な文字が来る 18/12/29(土)
                    preg_match_all('/[\d.]+/', $value['shipping_date'], $matches);
                    $value['shipping_date'] = date('Y-m-d', mktime(0, 0, 0, $matches[0][1], $matches[0][2], '20'.$matches[0][0]));
                }
                $value['del_flg'] = $data['del_flg'];
                $value['order_id'] = $data['order_id'];
                $value['create_date'] = self::convertTz($data['create_date']);
                $value['update_date'] = self::convertTz($data['update_date']);
                $value['shipping_commit_date'] = self::convertTz($data['commit_date']);

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
    private function fix24ProductsClass($em, $tmpDir)
    {
        if (($handle = fopen($tmpDir.'dtb_products_class.csv', 'r')) !== false) {
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));
            $keySize = count($key);

            $i = -1;
            while (($row = fgetcsv($handle)) !== false) {
                // 1行目をkeyとした配列を作る
                $data = $this->convertNULL(array_combine($key, $row));
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

            if (!empty($add_value)) {
                $fpcsv = fopen($tmpDir.'dtb_products_class.csv', 'a');
                foreach ($add_value as $row) {
                    fputcsv($fpcsv, array_values($row));
                }
                fclose($fpcsv);
            }
        }
    }

    private function fix211classCombination($em, $platform, $tmpDir)
    {
        if (($handle = fopen($tmpDir.'dtb_class_combination.csv', 'r')) !== false) {
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));
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

            $builder = new BulkInsertQuery($em, 'dtb_class_combination');
            $builder->setColumns(['class_combination_id', 'parent_class_combination_id', 'classcategory_id', 'level']);

            $i = 1;
            $batchSize = 20;
            while (($row = fgetcsv($handle)) !== false) {
                // 1行目をkeyとした配列を作る
                $data = $this->convertNULL(array_combine($key, $row));

                if (!$data['parent_class_combination_id']) {
                    $data['parent_class_combination_id'] = null;
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

        $stmt = $em->query('
        SELECT
        class_combination_id
        , (select classcategory_id from dtb_class_combination where class_combination_id = c1.parent_class_combination_id) as classcategory_id1
        , classcategory_id as classcategory_id2
        FROM dtb_class_combination as c1
        where parent_class_combination_id is not null
        ');
        $all = $stmt->fetchAllAssociative();

        $this->dtb_class_combination = [];
        foreach ($all as $line) {
            $this->dtb_class_combination[$line['class_combination_id']] = $line;
        }

        $stmt = $em->query('
        SELECT
        class_combination_id
        , classcategory_id as classcategory_id1
        , NULL as classcategory_id2
        FROM dtb_class_combination as c1
        where parent_class_combination_id is null
        ');
        $all = $stmt->fetchAllAssociative();

        foreach ($all as $line) {
            $this->dtb_class_combination[$line['class_combination_id']] = $line;
        }
    }

    private function saveStock($em)
    {
        $tableName = 'dtb_product_stock';
        $columns = $em->getSchemaManager()->listTableColumns($tableName);

        $listTableColumns = [];
        foreach ($columns as $column) {
            $listTableColumns[] = $column->getName();
        }

        $builder = new BulkInsertQuery($em, $tableName);
        $builder->setColumns($listTableColumns);

        $em->exec('DELETE FROM '.$tableName);

        $i = 1;
        $batchSize = 20;
        foreach ($this->stock as $product_class_id => $stock) {
            $data['id'] = $i;
            $data['product_class_id'] = $product_class_id;
            $data['creator_id'] = null; // 固定 ?
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

    private function saveProductImage($em)
    {
        $tableName = 'dtb_product_image';
        $columns = $em->getSchemaManager()->listTableColumns($tableName);

        $listTableColumns = [];
        foreach ($columns as $column) {
            $listTableColumns[] = $column->getName();
        }

        $builder = new BulkInsertQuery($em, $tableName);
        $builder->setColumns($listTableColumns);

        $em->exec('DELETE FROM '.$tableName);

        $i = 1;
        $batchSize = 20;
        foreach ($this->product_images as $product_id => $file_names) {
            foreach ($file_names as $image_id => $file_name) {
                $data['id'] = $i;
                $data['product_id'] = $product_id;
                $data['creator_id'] = null;
                $data['file_name'] = $file_name;
                $data['sort_no'] = $image_id + 1;

                $data['create_date'] = date('Y-m-d H:i:s');
                $data['discriminator_type'] = 'productimage';

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

    // 2.4.4から
    private function cutOff24($tmpDir, $csvName)
    {
        $tbl_flg = false;
        $col_flg = false;

        if (($handle = fopen($tmpDir.$csvName, 'r')) !== false) {
            $fpcsv = '';
            while (($row = fgetcsv($handle)) !== false) {
                //空白行のときはテーブル変更
                if (count($row) <= 1 and $row[0] == '') {
                    $tbl_flg = false;
                    $col_flg = false;
                    $enablePoint = false;
                    $key = [];
                    $i = 1;

                    continue;
                }

                // テーブルフラグがたっていない場合にはテーブル名セット
                if (!$tbl_flg) {
                    // 特定のテーブルのみ
                    switch ($row[0]) {
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
                        case 'dtb_shipping':
                        case 'dtb_shipment_item':
                        case 'dtb_mail_history':
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

    private function setIdSeq($em, $tableName)
    {
        $max = $em->fetchOne('SELECT coalesce(max(id), 0) + 1  FROM '.$tableName);
        $seq = $tableName.'_id_seq';
        $count = $em->fetchOne("select count(*) from pg_class where relname = '$seq';");
        if ($count) {
            $em->exec("SELECT setval('$seq', $max);");
        }
    }

    private function saveOrder($em, $csvDir)
    {
        // 会員系
        if (file_exists($csvDir.'dtb_order.csv') && filesize($csvDir.'dtb_order.csv') > 0) {
            $em->beginTransaction();

            $platform = $em->getDatabasePlatform()->getName();

            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 0;');
                $em->exec("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"); // STRICT_TRANS_TABLESを無効にする。
            } else {
                $em->exec('SET session_replication_role = replica;'); // need super user
            }

            // 2.4には存在しないデータ
            if (!$this->flag_244) {
                $this->saveToO($em, $csvDir, 'mtb_device_type', null, true);
            }
            // todo mtb_order_status.display_order_count
            $this->saveToO($em, $csvDir, 'mtb_device_type', null, true);

            if ($this->flag_4) {
                $this->saveToP($em, $csvDir, 'mtb_order_status', null, true);
                $this->saveToP($em, $csvDir, 'mtb_order_status_color', null, true);
                $this->saveToP($em, $csvDir, 'mtb_order_item_type', null, true);
                $this->saveToO($em, $csvDir, 'dtb_delivery_time');
                $this->saveToO($em, $csvDir, 'dtb_delivery');
                $this->saveToO($em, $csvDir, 'dtb_delivery_fee');
                $this->saveToO($em, $csvDir, 'dtb_mail_history');
            } else if ($this->flag_3) {
                $this->saveToO($em, $csvDir, 'dtb_delivery_time');
                $this->saveToO($em, $csvDir, 'dtb_delivery');
                $this->saveToO($em, $csvDir, 'dtb_delivery_fee');
                $this->saveToO($em, $csvDir, 'dtb_mail_history');
            } else {
                $this->saveToO($em, $csvDir, 'dtb_delivtime', 'dtb_delivery_time');
                $this->saveToO($em, $csvDir, 'dtb_deliv', 'dtb_delivery');
                $this->saveToO($em, $csvDir, 'dtb_delivfee', 'dtb_delivery_fee');
                $this->saveToO($em, $csvDir, 'dtb_mail_history', 'dtb_mail_history');
            }

            // fixme dtb_delivery_time のあとにやらなければダメ
            $this->saveToO($em, $csvDir, 'dtb_order');
            $this->saveToO($em, $csvDir, 'dtb_shipping');
            $this->saveToO($em, $csvDir, 'dtb_payment');

            if (!isset($this->product_class_id)) {
                sleep(5);
            }
            // todo 商品別税率設定
            $this->saveToO($em, $csvDir, 'dtb_tax_rule', null, true); // 税率0にしている場合がある

            // todo ダウンロード販売の処理
            if ($this->flag_4 == false) {
                $this->saveToO($em, $csvDir, 'dtb_order_detail', 'dtb_order_item', true);
            } else {
                // v4
                $this->saveToO($em, $csvDir, 'dtb_order_item');
                $this->saveToO($em, $csvDir, 'dtb_order_pdf');
                $this->saveToO($em, $csvDir, 'dtb_payment_option');
            }

            if (!empty($this->order_item)) {
                $this->saveOrderItem($em);
            }

            if ($this->flag_4 == false) {
                // 支払いは基本移行しない
                $em->exec('DELETE FROM dtb_payment_option');
            }


            if ($platform == 'mysql') {
                $em->exec('SET FOREIGN_KEY_CHECKS = 1;');
            } else {
                $this->setIdSeq($em, 'dtb_order');
                $this->setIdSeq($em, 'dtb_order_item');
                $this->setIdSeq($em, 'dtb_shipping');
                $this->setIdSeq($em, 'dtb_payment');
                $this->setIdSeq($em, 'dtb_delivery');
                $this->setIdSeq($em, 'dtb_delivery_fee');
                $this->setIdSeq($em, 'dtb_delivery_time');
                $this->setIdSeq($em, 'dtb_tax_rule');
                $this->setIdSeq($em, 'dtb_mail_history');
            }

            // イレギュラー対応
            $em->exec('UPDATE dtb_order SET order_status_id = NULL WHERE order_status_id not in (select id from mtb_order_status)');

            $em->commit();

            $this->addSuccess('受注データを登録しました。', 'admin');
        } else {
            $this->addDanger('受注データが見つかりませんでした', 'admin');
        }
    }

    private function saveToO($em, $tmpDir, $csvName, $tableName = null, $allow_zero = false, $i = 1)
    {
        $tableName = ($tableName) ? $tableName : $csvName;
        $this->resetTable($em, $tableName);

        if (file_exists($tmpDir.$csvName.'.csv') == false) {
            // 無視する
            //$this->addDanger($csvName.'.csv が見つかりませんでした' , 'admin');
            return;
        }
        if (filesize($tmpDir.$csvName.'.csv') == 0) {
            // 無視する
            $this->addWarning($csvName.'.csv のデータがありません。', 'admin');

            return;
        }

        if (($handle = fopen($tmpDir.$csvName.'.csv', 'r')) !== false) {
            // 文字コード問題が起きる可能性が高いので後で調整が必要になると思う
            $key = fgetcsv($handle);
            // phpmyadminのcsvに余計なスペースが入っているので取り除く
            $key = array_filter(array_map('trim', $key));
            $keySize = count($key);

            $columns = $em->getSchemaManager()->listTableColumns($tableName);
            $listTableColumns = [];
            foreach ($columns as $column) {
                $listTableColumns[] = $column->getName();
            }

            $builder = new BulkInsertQuery($em, $tableName);
            $builder->setColumns($listTableColumns);

            $batchSize = 20;

            while (($row = fgetcsv($handle)) !== false) {
                $value = [];

                // 1行目をkeyとした配列を作る
                $data = $this->convertNULL(array_combine($key, $row));

                // order_ の文字を除去
                foreach ($data as $k => $v) {
                    if ($tableName == 'dtb_order') {
                        $data[str_replace('order_', '', $k)] = $v;
                    } elseif ($tableName == 'dtb_shipping') {
                        $data[str_replace('shipping_', '', $k)] = $v;
                    }
                }

                // 3の差を埋める
                if ($this->flag_3) {
                    if (isset($data['delivery_id'])) {
                        $data['deliv_id'] = $data['delivery_id'];
                    }
                    if ('dtb_order' === $tableName && isset($data['delivery_fee_total'])) {
                        $data['deliv_fee'] = $data['delivery_fee_total'];
                    }
                }

                // Schemaにあわせた配列を作成する
                foreach ($listTableColumns as $column) {
                    if ($this->flag_4 == true) {
                        if ($column == 'use_point') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'creator_id') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 1;
                        } elseif ($column == 'tax_adjust') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'tax') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'tax_rule') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'tax_rate') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'fee') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'create_date' || $column == 'update_date') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : date('Y:m:d H:i:s');
                        } elseif ($column == 'payment_date' || $column == 'order_date' || $column == 'shipping_date') {
                            $value[$column] = (!empty($data[$column])) ? $data[$column] : null;
                        } elseif ($column == 'add_point') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'visible') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($column == 'quantity') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 0;
                        } elseif ($allow_zero) {
                            $value[$column] = isset($data[$column]) ? $data[$column] : null;
                        } else {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        }
                    } else {
                        if ($column == 'id' && $tableName == 'dtb_payment') {
                            $value[$column] = $data['payment_id'];
                        } elseif ($column == 'id' && $tableName == 'dtb_delivery') {
                            $value[$column] = $data['deliv_id'];
                        } elseif ($column == 'id' && $tableName == 'dtb_delivery_fee') {
                            $value[$column] = $i; // todo
                        } elseif ($column == 'id' && $tableName == 'dtb_delivery_time') {
                            // deliv_idとtime_idで複合主キーだったのが、idのみの主キーとなったため、連番で付与する.
                            $value[$column] = $i;

                            // dtb_order.deliv_idとdtb_shipping.time_idでお届け時間を特定するため、ここで保持しておく.
                            $this->delivery_time[$data['deliv_id']][$data['time_id']] = $i;
                        } elseif ($column == 'order_status_id') {
                            // 退会が追加された
                            $value[$column] = ($data['del_flg'] == 1) ? '3' : $data['status'];

                            // 4系に存在しないstatusなので
                            if ($data['status'] == 2) {
                                $value[$column] = 4;
                            }
                            if ($data['status'] == '') {
                                $value[$column] = 3;
                            }
                        } elseif ($column == 'message' || $column == 'note') {
                            $value[$column] = empty($data[$column]) ? null : mb_substr($data[$column], 0, 4000);
                        } elseif ($column == 'postal_code') {
                            $value[$column] = mb_substr(mb_convert_kana($data['zip01'].$data['zip02'], 'a'), 0, 8);
                            if (empty($value[$column])) {
                                $value[$column] = null;
                            }
                        } elseif ($column == 'phone_number') {
                            $value[$column] = mb_substr(mb_convert_kana($data['tel01'].$data['tel02'].$data['tel03'], 'a'), 0, 14); //14文字制限
                            if (empty($value[$column])) {
                                $value[$column] = null;
                            }
                        } elseif ($column == 'sex_id') {
                            $value[$column] = empty($data['sex']) ? null : $data['sex'];
                        } elseif ($column == 'job_id') {
                            $value[$column] = empty($data['job']) ? null : $data['job']; // 0が入っている場合あり?
                        } elseif ($column == 'pref_id') {
                            $value[$column] = empty($data['pref']) ? null : $data['pref'];
                        } elseif ($column == 'delivery_fee_total') {
                            $value[$column] = empty($data['deliv_fee']) ? 0 : $data['deliv_fee'];

                        // --> shipping
                        } elseif ($column == 'delivery_date') {
                            $value[$column] = empty($data['date']) ? null : $data['date'];
                        } elseif ($column == 'shipping_date') {
                            $value[$column] = empty($data['commit_date']) ? null : self::convertTz($data['commit_date']);
                        } elseif ($column == 'visible' /*&& $tableName == 'dtb_payment'*/) {
                            $value[$column] = 0;

                        // --> deliv
                        } elseif ($column == 'sale_type_id') {
                            $value[$column] = isset($data['product_type_id']) ? $data['product_type_id'] : 1;
                        } elseif ($column == 'description') {
                            $value[$column] = isset($data['remark']) ? $data['remark'] : null;
                        } elseif ($column == 'delivery_id') {
                            $value[$column] = isset($data['deliv_id']) ? $data['deliv_id'] : null;
                        } elseif ($column == 'delivery_time') {
                            if (isset($data['deliv_time'])) {
                                $value[$column] = $data['deliv_time'];
                            } elseif (isset($data['delivery_time']) && strlen($data['delivery_time']) > 0) {
                                $value[$column] = $data['delivery_time'];
                            } else {
                                $value[$column] = null;
                            }
                        } elseif ($column == 'fee') {
                            $value[$column] = !empty($data['fee']) ? $data['fee'] : 0;
                        // --> payment
                        } elseif ($column == 'fixed') {
                            $value[$column] = 1;
                        } elseif ($column == 'rule_max') {
                            if ($this->flag_3) {
                                $value[$column] = isset($data['rule_max']) && strlen($data['rule_max']) > 0 ? $data['rule_max'] : null;
                            } else {
                                // 2.13
                                $value[$column] = !empty($data['upper_rule']) ? $data['upper_rule'] : null;
                            }
                        } elseif ($column == 'rule_min') {
                            if ($this->flag_3) {
                                $value[$column] = isset($data['rule_min']) && strlen($data['rule_min']) > 0 ? $data['rule_min'] : null;
                            } else {
                                // 2.13
                                $value[$column] = !empty($data['rule_max']) ? $data['rule_max'] :
                                    (!empty($data['rule']) ? $data['rule'] : null ) ;
                            }
                        // --> dtb_order_item
                        } elseif ($column == 'class_category_name1') {
                            $value[$column] = isset($data['classcategory_name1']) && strlen($data['classcategory_name1']) > 0 ? $data['classcategory_name1'] : null;
                        } elseif ($column == 'class_category_name2') {
                            $value[$column] = isset($data['classcategory_name2']) && strlen($data['classcategory_name2']) > 0 ? $data['classcategory_name2'] : null;
                        } elseif ($column == 'name01' || $column == 'name02') {
                            $value[$column] = empty($data[$column]) ? 'Not null violation' : $data[$column];
                        } elseif ($column == 'sort_no' && $tableName == 'dtb_shipping') {
                            $value[$column] = $data['id'];
                        } elseif ($column == 'sort_no') {
                            $value[$column] = isset($data['rank']) ? $data['rank'] : 0;
                        } elseif ($column == 'create_date' || $column == 'update_date') {
                            $value[$column] = (isset($data[$column]) && $data[$column] != '0000-00-00 00:00:00') ? self::convertTz($data[$column]) : date('Y-m-d H:i:s');
                        } elseif ($column == 'payment_date') {
                            $value[$column] = (!empty($data[$column]) && $data[$column] != '0000-00-00 00:00:00') ? self::convertTz($data[$column]) : null;
                        } elseif ($column == 'creator_id') {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : 1;
                        } elseif ($column == 'charge' || $column == 'use_point' || $column == 'add_point' || $column == 'discount' || $column == 'total' || $column == 'subtotal' || $column == 'tax' || $column == 'payment_total') {
                            $value[$column] = !empty($data[$column]) ? (int) $data[$column] : 0;
                        } elseif ($column == 'tax_adjust') {
                            $value['tax_adjust'] = 0; // 0固定
                        } elseif ($column == 'discriminator_type') {
                            $search = ['dtb_', 'mtb_', '_'];
                            $value[$column] = str_replace($search, '', $tableName);
                        } elseif ($allow_zero) {
                            $value[$column] = isset($data[$column]) ? $data[$column] : null;
                        } else {
                            $value[$column] = !empty($data[$column]) ? $data[$column] : null;
                        }
                    }
                }

                // 別テーブルからのデータなど
                switch ($tableName) {
                    case 'dtb_order':
                        if ($this->flag_4 == false) {
                            // 配送ID
                            if (isset($data['deliv_id'])) {
                                $this->delivery_id[$data['id']] = $data['deliv_id'];
                            }
                            $value['order_no'] = $data['id'];
                            $value['order_date'] = self::convertTz($data['create_date']);
                            $value['currency_code'] = 'JPY'; // とりあえず固定

                            // 3は delivery_fee_total
                            if (isset($data['deliv_fee']) && $data['deliv_fee'] > 0) {
                                $this->order_item[$data['id']]['deliv_fee'] = [
                                    'price' => $data['deliv_fee'],
                                    'order_date' => $value['order_date'],
                                ];
                            }
                            if ($data['charge'] > 0) {
                                $this->order_item[$data['id']]['charge'] = [
                                    'price' => $data['charge'],
                                    'order_date' => $value['order_date'],
                                ];
                            }
                            if ($data['discount'] > 0) {
                                $this->order_item[$data['id']]['discount'] = [
                                    'price' => $data['discount'],
                                    'order_date' => $value['order_date'],
                                ];
                            }
                            // todo 3はプラグイン
                            if (isset($data['use_point']) && $data['use_point'] > 0) {
                                $this->order_item[$data['id']]['use_point'] = [
                                    'price' => $data['use_point'],
                                    'order_date' => $value['order_date'],
                                ];
                            }
                        }

                        // shippingに紐付けるデータを保持
                        $this->shipping_order[$data['id']] = $data;

                        break;

                    case 'dtb_shipping':
                        if ($this->flag_4 == false) {
                            $value['id'] = $i;
                            $this->shipping_id[$data['order_id']][$data['shipping_id']] = $i;

                            if ($this->flag_3) {
                                if (isset($data['delivery_id']) & strlen($data['delivery_id']) > 0) {
                                    $value['delivery_id'] = $data['delivery_id'];
                                } else {
                                    $value['delivery_id'] = null;
                                }
                                if (isset($data['time_id']) && strlen($data['time_id']) > 0) {
                                    $value['time_id'] = $this->delivery_time[$data['delivery_id']][$data['time_id']];
                                }
                            } else {
                                $value['delivery_id'] = !empty($this->delivery_id[$value['order_id']]) ? $this->delivery_id[$value['order_id']] : null;
                                $value['delivery_time'] = empty($data['time']) ? null : $data['time'];
                                if (isset($data['time_id']) && strlen($data['time_id']) > 0) {
                                    if (!empty($this->delivery_time)) {
                                        $value['time_id'] = $this->delivery_time[$value['delivery_id']][$data['time_id']];
                                    }
                                }
                                // dtb_shipping.shipping_commit_dateが空の場合は、dtb_order.commit_dateを使用
                                if (!empty($data['shipping_commit_date'])) {
                                    $value['shipping_date'] = $data['shipping_commit_date'];
                                } elseif (!empty($this->shipping_order[$data['order_id']]['commit_date'])) {
                                    $value['shipping_date'] = $this->shipping_order[$data['order_id']]['commit_date'];
                                }
                            }
                        }

                        break;

                    case 'dtb_tax_rule':
                        if ($this->flag_4 == false) {
                            $value['id'] = $data['tax_rule_id'];
                            $value['apply_date'] = self::convertTz($data['apply_date']);
                            $value['rounding_type_id'] = $data['calc_rule'];
                            $value['tax_adjust'] = 0;
                        }

                        if (isset($data['pref_id']) && $data['pref_id'] === '0') {
                            $value['pref_id'] = null;
                        }
                        if (isset($data['country_id']) && $data['country_id'] === '0') {
                            $value['country_id'] = null;
                        }
                        if (isset($data['product_id']) && $data['product_id'] === '0'
                            && isset($data['product_class_id']) && $data['product_class_id'] === '0') {
                            $value['product_id'] = null;
                            $value['product_class_id'] = null;
                        }

                        // 3系対応
                        if (!$value['pref_id']) {
                            $value['pref_id'] = null;
                        }
                        if (!$value['country_id']) {
                            $value['country_id'] = null;
                        }
                        if (!$value['product_id']) {
                            $value['product_id'] = null;
                        }
                        if (!$value['product_class_id']) {
                            $value['product_class_id'] = null;
                        }

                        // 基本税率を保持しておく(送料等の明細を作成するタイミングで利用する)
                        if ($value['product_id'] === null && $value['product_class_id'] === null) {
                            $this->tax_rule[$value['apply_date']] = [
                                'rounding_type_id' => $value['rounding_type_id'],
                                'tax_rate' => $data['tax_rate'],
                                'apply_date' => $value['apply_date'],
                            ];
                        }
                        krsort($this->tax_rule);
                        break;

                    case 'dtb_order_item':
                        if ($this->flag_4 == false) {
                            if (isset($data['order_detail_id'])) {
                                $value['id'] = $data['order_detail_id'];
                            } else {
                                $value['id'] = $i; // 2.4.4
                            }
                            // dtb_order_detail.tax_ruleははdtb_tax_rule.calc_ruleの値
                            $value['rounding_type_id'] = isset($data['tax_rule'])
                                ? $data['tax_rule']
                                : $this->baseinfo['tax_rule'];

                            $value['tax_type_id'] = 1;
                            $value['tax_display_type_id'] = 1;

                            // 4.0.3でtax_rule_idはdeprecated.
                            $value['tax_rule_id'] = null;

                            // 2.4.4, 2.11, 2.12
                            if (isset($this->baseinfo) && !empty($this->baseinfo)) {
                                $value['tax_rate'] = $data['tax_rate'] = $this->baseinfo['tax'];
                                $data['point_rate'] = $this->baseinfo['point_rate'];
                            }

                            // 2.4.4
                            if ($this->flag_244) {
                                $value['product_class_id'] = $this->product_class_id[$data['product_id']][$data['classcategory_id1']][$data['classcategory_id2']];
                            }

                            if (isset($data['price']) && isset($data['tax_rate'])) {
                                if ($value['rounding_type_id'] == 2) {
                                    $round = 'floor';
                                } elseif ($value['rounding_type_id'] == 3) {
                                    $round = 'ceil';
                                } else {
                                    $round = 'round';
                                }
                                // Warning: A non-numeric value encountered
                                $value['tax'] = $round((int)$data['price'] * (int)$data['tax_rate'] / 100);
                            } else {
                                $value['tax'] = 0;
                            }

                            $value['order_item_type_id'] = 1; // 商品で固定する
                            $value['currency_code'] = 'JPY'; // とりあえず固定

                            if ($this->flag_3) {
                                // 1行目だけを移行する
                                $value['shipping_id'] = array_values($this->shipping_id[$data['order_id']])[0];
                            } else {
                                if (isset($this->shipping_id[$data['order_id']][0])) {
                                    $value['shipping_id'] = $this->shipping_id[$data['order_id']][0];
                                } else {
                                    $value['shipping_id'] = null; // ダウンロード販売
                                }
                            }
                        }
                        break;

                    case 'dtb_mail_history':
                        if ($this->flag_4 == false) {
                            $value['id'] = $data['send_id'];
                            $value['order_id'] = $data['order_id'];
                            $value['send_date'] = self::convertTz($data['send_date']);
                            $value['mail_subject'] = $data['subject'];
                            $value['mail_body'] = $data['mail_body'];
                        }

                        break;

                    case 'dtb_payment':
                        $value['method_class'] = 'Eccube\Service\Payment\Method\Cash';
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

    private function saveOrderItem($em)
    {
        $tableName = 'dtb_order_item';
        $columns = $em->getSchemaManager()->listTableColumns($tableName);

        $listTableColumns = [];
        foreach ($columns as $column) {
            $listTableColumns[] = $column->getName();

            if ($column->getName() == 'tax_adjust') {
                $data[$column->getName()] = 0; // 4.0.3 以降に対する対応
            } else {
                $data[$column->getName()] = null;
            }
        }

        $builder = new BulkInsertQuery($em, $tableName);
        $builder->setColumns($listTableColumns);

        $i = $em->fetchOne('SELECT max(id) + 1  FROM '.$tableName);
        $batchSize = 20;
        foreach ($this->order_item as $order_id => $type) {
            foreach ($type as $key => $value) {
                $tax_rule = $this->getTaxRule($value['order_date']);
                $data['tax_rate'] = $tax_rule['tax_rate'];
                $data['rounding_type_id'] = $tax_rule['rounding_type_id'];
                $data['shipping_id'] = null;

                switch ($key) {
                case 'deliv_fee':
                    $data['order_item_type_id'] = 2;
                    $data['product_name'] = '送料';
                    $data['price'] = $value['price'];
                    $data['tax_type_id'] = 1; // 課税
                    $data['tax_display_type_id'] = 2; // 税込表示
                    if (isset($this->shipping_id[$order_id][0])) {
                        $data['shipping_id'] = $this->shipping_id[$order_id][0];
                    }
                    break;
                case 'charge':
                    $data['order_item_type_id'] = 3;
                    $data['product_name'] = '手数料';
                    $data['price'] = $value['price'];
                    $data['tax_type_id'] = 1; // 課税
                    $data['tax_display_type_id'] = 2; // 税込表示
                    break;
                case 'discount':
                    $data['order_item_type_id'] = 4;
                    $data['product_name'] = '割引';
                    $data['price'] = $value['price'] * -1;
                    $data['tax_type_id'] = 1; // 課税
                    $data['tax_display_type_id'] = 2; // 税込表示
                    break;
                case 'use_point':
                    $data['order_item_type_id'] = 6;
                    $data['product_name'] = 'ポイント';
                    $data['price'] = $value['price'] * -1; // use_pointはポイント数のため、正確な値はだせない.ここでは1pt1円として登録する.
                    $data['tax_type_id'] = 2;   // 不課税
                    $data['tax_display_type_id'] = 2; // 税込表示
                    $data['tax_rate'] = 0;
                    break;
                }

                if ($data['rounding_type_id'] == 2) {
                    $round = 'floor';
                } elseif ($data['rounding_type_id'] == 3) {
                    $round = 'ceil';
                } else {
                    $round = 'round';
                }
                $data['tax'] = $round($data['price'] * $data['tax_rate'] / 100) * 1;
                //$data['tax_adjust'] = 0; // 4.0.2でエラーになる
                $data['quantity'] = 1;
                $data['id'] = $i;
                $data['order_id'] = $order_id;
                $data['currency_code'] = 'JPY';
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

    private function checkUploadSize()
    {
        if (!$filesize = ini_get('upload_max_filesize')) {
            $filesize = '5M';
        }

        if ($postsize = ini_get('post_max_size')) {
            return min($filesize, $postsize);
        } else {
            return $filesize;
        }
    }

    private function resetTable($em, $tableName)
    {
        $platform = $em->getDatabasePlatform()->getName();

        if ($platform == 'mysql') {
            $em->exec('DELETE FROM '.$tableName);
        } else {
            $em->exec('DELETE FROM '.$tableName);
        }
    }


    // タイムゾーンの変換
    private function convertTz($datetime)
    {
        $date = new \DateTime($datetime, new \DateTimeZone($this->eccubeConfig->get('timezone')));
        $date->setTimezone(new \DateTimeZone('UTC'));

        return $date->format($this->em->getDatabasePlatform()->getDateTimeTzFormatString());
    }

    private function getTaxRule($order_date)
    {
        foreach ($this->tax_rule as $apply_date => $value) {
            if ($apply_date < $order_date) {
                return $value;
            }
        }

        return array_values($this->tax_rule)[0];
    }

    private function fixDeletedProduct($em)
    {
        $sql = 'UPDATE
            dtb_product_class
        SET
            visible = true
        WHERE
            id IN(
                SELECT
                    product_class_id
                FROM
                    (
                        SELECT
                            t1.id AS product_class_id
                        FROM
                            dtb_product_class AS t1
                            LEFT JOIN
                                dtb_product AS t2
                            on  t1.product_id = t2.id
                        WHERE
                            t2.product_status_id = 3
                        AND t1.visible = false
                    ) AS t
            )';

        $em->exec($sql);
    }


    /**
     * "NULL" -> null
     *
     * @param mixed $data
     * @access private
     * @return array
     */
    private function convertNULL($data)
    {
        foreach ($data as &$v) {
            if ($v === "NULL") {
                $v = null;
            }
        }
        return $data;
    }
}
