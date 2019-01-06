/*
 * Copyright 2017 Celeral <netlet@celeral.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.celeral.netlet.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.validation.constraints.NotNull;

import com.esotericsoftware.kryo.serializers.JavaSerializer;

import org.junit.Assert;
import org.junit.Test;

import com.celeral.netlet.AbstractServer;
import com.celeral.netlet.DefaultEventLoop;
import com.celeral.netlet.rpc.ConnectionAgent.SimpleConnectionAgent;
import com.celeral.netlet.rpc.methodserializer.ExternalizableMethodSerializer;
import com.celeral.netlet.util.Throwables;

/**
 *
 * @author Chetan Narsude  <chetan@apache.org>
 */
public class RPC2Test
{
  public static final String GREETING = "Hello World!";
  public static final String CHARSET_UTF_8 = "UTF-8";

  public static byte[] encrypt(PublicKey key, byte[] plaintext) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
  {
    Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA1AndMGF1Padding");
    cipher.init(Cipher.ENCRYPT_MODE, key);
    return cipher.doFinal(plaintext);
  }

  public static byte[] decrypt(PrivateKey key, byte[] ciphertext) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
  {
    Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA1AndMGF1Padding");
    cipher.init(Cipher.DECRYPT_MODE, key);
    return cipher.doFinal(ciphertext);
  }

  private void authenticate(ProxyClient client, Identity identity) throws IOException
  {
    Authenticator authenticator = client.create(identity, Authenticator.class);
    try {
      final String alias = Integer.toString(new Random(System.currentTimeMillis()).nextInt(clientKeys.keys.size()));

      PublicKey clientKey = clientKeys.keys.get(alias).getPublic();
      PublicKey serverKey = authenticator.getPublicKey(alias, clientKey);

      try {
        byte[] challengePhrase = GREETING.concat(alias).getBytes(CHARSET_UTF_8);
        byte[] responsePhrase = decrypt(clientKeys.keys.get(alias).getPrivate(), authenticator.challenge(encrypt(serverKey, challengePhrase)));
        Assert.assertArrayEquals(challengePhrase, responsePhrase);
      }
      catch (UnsupportedEncodingException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException ex) {
        throw new RuntimeException(ex);
      }
    }
    finally {
      ((Closeable)Proxy.getInvocationHandler(authenticator)).close();
    }
  }

  public static interface Authenticator
  {
    PublicKey getPublicKey(String alias, PublicKey clientPublicKey);

    byte[] challenge(byte[] encryptedId);
  }

  public static class AuthenticatorImpl implements Authenticator
  {
    private static final char[] PASSWORD = "Test123".toCharArray();

    HashMap<String, PublicKey> publicKeys = new HashMap<>();

    KeyStore keystore;
    KeyPair master;

    public AuthenticatorImpl()
    {
      try {
        keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        keystore.load(null, PASSWORD);
      }
      catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException ex) {
        throw Throwables.throwFormatted(ex,
                                        IllegalStateException.class,
                                        "Unable to create a keystore to store they keys!");
      }

      try {
        master = createMasterKeys();
      }
      catch (NoSuchAlgorithmException ex) {
        throw Throwables.throwFormatted(ex,
                                        IllegalStateException.class,
                                        "Unable to create a master key pair!");
      }
    }

    public void add(String alias, PublicKey entry)
    {
      publicKeys.put(alias, entry);
    }

    @Override
    public PublicKey getPublicKey(@NotNull String alias, @NotNull PublicKey clientPublicKey)
    {
      if (clientPublicKey.equals(publicKeys.get(alias))) {
        return master.getPublic();
      }

      return null;
    }

    @Override
    public byte[] challenge(byte[] encryptedId)
    {
      try {
        byte[] decrypt = decrypt(master.getPrivate(), encryptedId);
        return encrypt(publicKeys.get(new String(decrypt, CHARSET_UTF_8).substring(GREETING.length())), decrypt);
      }
      catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException | UnsupportedEncodingException ex) {
        throw new RuntimeException(ex);
      }
    }

    private KeyPair createMasterKeys() throws NoSuchAlgorithmException
    {
      KeyPairGenerator rsaGenerator = KeyPairGenerator.getInstance("RSA");
      rsaGenerator.initialize(2048);
      return rsaGenerator.generateKeyPair();
    }

  }

  public static class ClientKeys
  {
    HashMap<String, KeyPair> keys;

    public ClientKeys()
    {
      final int initialCapacity = 10;
      keys = new HashMap<>(initialCapacity);
      populateKeys(initialCapacity);
    }

    private void populateKeys(int count)
    {
      try {
        KeyPairGenerator rsaGenerator = KeyPairGenerator.getInstance("RSA");
        rsaGenerator.initialize(2048);

        for (int i = 0; i < count; i++) {
          keys.put(Integer.toString(i), rsaGenerator.generateKeyPair());
        }
      }
      catch (NoSuchAlgorithmException ex) {
        throw Throwables.throwFormatted(ex,
                                        IllegalStateException.class,
                                        "Unable to populate the keypairs for the clients");
      }
    }
  }

  public static class Server extends AbstractServer
  {
    private final Executor executor;

    public Server(Executor executor)
    {
      this.executor = executor;
    }

    @Override
    public ClientListener getClientConnection(SocketChannel client, ServerSocketChannel server)
    {
      ExecutingClient executingClient = new ExecutingClient(new Bean<Identity>()
      {
        AuthenticatorImpl authenticatorImpl = new AuthenticatorImpl();

        {
          for (Entry<String, KeyPair> entry : clientKeys.keys.entrySet()) {
            authenticatorImpl.add(entry.getKey(), entry.getValue().getPublic());
          }
        }

        @Override
        public Object get(Identity identifier)
        {
          return authenticatorImpl;
        }
      }, ExternalizableMethodSerializer.SINGLETON, executor);
      executingClient.addDefaultSerializer(sun.security.rsa.RSAPublicKeyImpl.class, new JavaSerializer());
      return executingClient;
    }

    @Override
    public void registered(SelectionKey key)
    {
      super.registered(key);
      synchronized (this) {
        notify();
      }
    }

  }

  static ClientKeys clientKeys = new ClientKeys();

  @Test
  public void testAuthenticator() throws IOException, InterruptedException
  {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      DefaultEventLoop el = DefaultEventLoop.createEventLoop("rpc");
      el.start();
      try {
        Server server = new Server(executor);
        el.start(new InetSocketAddress(0), server);

        try {
          SocketAddress si;
          synchronized (server) {
            while ((si = server.getServerAddress()) == null) {
              server.wait();
            }
          }

          ProxyClient client = new ProxyClient(new SimpleConnectionAgent(si, el),
                                               TimeoutPolicy.NO_TIMEOUT_POLICY,
                                               ExternalizableMethodSerializer.SINGLETON,
                                               executor);
          client.addDefaultSerializer(sun.security.rsa.RSAPublicKeyImpl.class, new JavaSerializer());
          Identity identity = new Identity();
          identity.name = "authenticator";
          authenticate(client, identity);
        }
        finally {
          el.stop(server);
        }
      }
      finally {
        el.stop();
      }

    }
    finally {
      executor.shutdown();
    }
  }

  public static class Identity
  {
    public String name;
  }

}
