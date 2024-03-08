<?php


namespace Plugin\DataMigration42\Tests\Web\Admin;


use Eccube\Common\Constant;
use Eccube\Entity\Customer;
use Eccube\Entity\Order;
use Eccube\Entity\Product;
use Eccube\Tests\Web\Admin\AbstractAdminWebTestCase;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\HttpFoundation\File\UploadedFile;
use Symfony\Component\HttpFoundation\Response;

class ConfigControllerTest extends AbstractAdminWebTestCase
{
    public function setUp() : void
    {
        parent::setUp();
    }

    public function tearDown() : void
    {
        parent::tearDown();
    }

    public function versionProvider()
    {
        return [
            ['2_11_5', 1, 0, 3],
            ['2_12_6', 1, 3, 2],
            ['2_13_5', 1, 3, 2],
            ['3_0_9', 1, 2, 6],
            ['3_0_18', 1, 2, 4],
            ['4_0_6', 1, 12, 20],
            ['4_1_2', 1, 12, 20],
        ];
    }

    /**
     * @dataProvider versionProvider
     */
    public function testバックアップファイルをアップロードできるかテスト($v, $c, $p, $o)
    {
        $container = self::getContainer();
        $project_dir = $container->getParameter('kernel.project_dir');

        $file = $project_dir.'/app/Plugin/DataMigration42/Tests/Fixtures/'.$v.'.tar.gz';
        $testFile = $project_dir.'/app/Plugin/DataMigration42/Tests/Fixtures/test.tar.gz';

        $fs = new Filesystem();
        $fs->copy($file, $testFile);

        $file = new UploadedFile($testFile, 'test.tar.gz', 'application/x-tar', null, true);

        $post =
            [
                'config' => [
                    Constant::TOKEN_NAME => 'dummy',
                    'import_file' => $file,
                ]
            ]
            ;

        // 2.11系のmysqlにはcreate tableが使われているので、商品を除外してテストする
        if ($v == '2_11_5' && $this->entityManager->getConnection()->getDatabasePlatform()->getName() === 'mysql') {
            $post['config']['customer_order_only'] = 1;
        }

        $this->client->request(
            'POST',
            $this->generateUrl('data_migration42_admin_config'),
            $post,
            ['config' => ['import_file' => $file]]
        );

        $customers = $this->entityManager->getRepository(Customer::class)->findAll();
        self::assertEquals($c, count($customers));

        if ($p > 0) {
            $products = $this->entityManager->getRepository(Product::class)->findAll();
            self::assertEquals($p, count($products));
        }

        $orders = $this->entityManager->getRepository(Order::class)->findAll();
        self::assertEquals($o, count($orders));
    }

}
