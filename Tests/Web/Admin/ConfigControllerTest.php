<?php


namespace Plugin\DataMigration4\Tests\Web\Admin;


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
    public function setUp()
    {
        parent::setUp();
    }

    public function tearDown()
    {
        parent::tearDown();
    }

    public function testv2_12のバックアップファイルをアップロードできるかテスト()
    {
        $project_dir = self::$container->getParameter('kernel.project_dir');

        $file = $project_dir.'/app/Plugin/DataMigration4/Tests/Fixtures/backup2_12.tar.gz';
        $testFile = $project_dir.'/app/Plugin/DataMigration4/Tests/Fixtures/test.tar.gz';

        $fs = new Filesystem();
        $fs->copy($file, $testFile);

        $file = new UploadedFile($testFile, 'test.tar.gz', 'application/x-tar', null, null, true);

        $this->client->request(
            'POST',
            $this->generateUrl('data_migration4_admin_config'),
            [
                'config' => [
                    Constant::TOKEN_NAME => 'dummy',
                    'import_file' => $file
                ]
            ]
        );

        self::assertEquals(Response::HTTP_FOUND, $this->client->getResponse()->getStatusCode());
        self::assertTrue($this->client->getResponse()->isRedirect($this->generateUrl('data_migration4_admin_config')));

        $customers = $this->entityManager->getRepository(Customer::class)->findAll();
        self::assertEquals(1, count($customers));

        $products = $this->entityManager->getRepository(Product::class)->findAll();
        self::assertEquals(3, count($products));

        $orders = $this->entityManager->getRepository(Order::class)->findAll();
        self::assertEquals(2, count($orders));
    }

    public function testv3のバックアップファイルをアップロードできるかテスト()
    {
        $project_dir = self::$container->getParameter('kernel.project_dir');

        $file = $project_dir.'/app/Plugin/DataMigration4/Tests/Fixtures/backup3.tar.gz';
        $testFile = $project_dir.'/app/Plugin/DataMigration4/Tests/Fixtures/test.tar.gz';

        $fs = new Filesystem();
        $fs->copy($file, $testFile);

        $file = new UploadedFile($testFile, 'test.tar.gz', 'application/x-tar', null, null, true);

        $this->client->request(
            'POST',
            $this->generateUrl('data_migration4_admin_config'),
            [
                'config' => [
                    Constant::TOKEN_NAME => 'dummy',
                    'import_file' => $file
                ]
            ]
        );

        self::assertEquals(Response::HTTP_FOUND, $this->client->getResponse()->getStatusCode());
        self::assertTrue($this->client->getResponse()->isRedirect($this->generateUrl('data_migration4_admin_config')));

        $customers = $this->entityManager->getRepository(Customer::class)->findAll();
        self::assertEquals(1, count($customers));

        $products = $this->entityManager->getRepository(Product::class)->findAll();
        self::assertEquals(2, count($products));

        $orders = $this->entityManager->getRepository(Order::class)->findAll();
        self::assertEquals(1, count($orders));
    }

}
