# データ移行プラグイン for EC-CUBE4
ec-cube2系から出力出来るバックアップデータを利用して、2系から4系へのデータ移行をするプラグイン

## 移行出来るデータ
### 会員データ
 - dtb_customer
 - dtb_customer_address
 - mtb_sex
 - mtb_job
### 管理者データ
 - dtb_member
 - mtb_authority
### 商品データ
 - dtb_product
 - dtb_product_class
 - dtb_class_category
 - dtb_class_name
 - mtb_sale_type
### カテゴリデータ
 - dtb_category
 - dtb_product_category
### 受注データ
 - dtb_order
 - dtb_shipping
 - dtb_order_item
### 支払い方法
 - dtb_payment
### 配送方法
 - dtb_delivery
 - dtb_delivery_fee
 - dtb_delivery_time
### 税設定
 - dtb_tax_rule

## 移行出来ないデータ
### 商品画像
 - dtb_product_image
### カート
 - dtb_cart
 - dtb_cart_item
### 決済モジュール
 - dtb_payment_option


## 注意点
プラグイン内でcomposerを使用しているため、オーナーズストア経由のインストールが必要になります

## License
[LGPL](LICENSE)
