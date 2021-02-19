<?php

namespace Plugin\DataMigration4\Tests\Web\Admin;

use Eccube\Common\Constant;
use Eccube\Tests\Web\Admin\AbstractAdminWebTestCase;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\HttpFoundation\File\UploadedFile;

class ConfigControllerTest extends AbstractAdminWebTestCase
{
    public function setUp()
    {
        parent::setUp(); // TODO: Change the autogenerated stub

        $member = $this->createMember();
        $this->loginTo($member);
    }

    public function tearDown()
    {
        parent::tearDown(); // TODO: Change the autogenerated stub
    }

    public function testCsvUpload()
    {
        $pluginDir = $this->container->getParameter('kernel.project_dir').'/app/Plugin';
        $file = new UploadedFile($pluginDir . '/DataMigration4/Tests/backup2_12.tar.gz', 'backup2_12.tar.gz', 'application/x-tar', null, null, true);

        $crawler = $this->client->request(
            'POST',
            $this->generateUrl('data_migration4_admin_config'),
            [
                'data_migration_plugin_csv_import' => [
                    '_token' => 'dummy',
                    'import_file' => $file,
                ],
            ],
            ['import_file' => $file]
        );

        //$this->assertRegexp(
        //    '/会員データ登録しました。/u',
        //    $crawler->filter('div.alert-success')->text()
        //);

        $crawler = $this->client->request(
            'POST',
            $this->generateUrl('admin_customer'),
            ['admin_search_customer' => ['_token' => 'dummy']]
        );
        $this->assertTrue($this->client->getResponse()->isSuccessful());

        $this->expected = '検索結果：1件が該当しました';
        $this->actual = $crawler->filter('div.c-outsideBlock__contents.mb-5 > span')->text();
        $this->verify();

    }
}