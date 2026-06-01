import bcrypt from 'bcryptjs';

export async function verifyAdminCredential(username: string, password: string) {
  const expectedUsername = process.env.ADMIN_USERNAME || 'admin-pebri';
  const configuredHash = process.env.ADMIN_PASSWORD_HASH;
  const fallbackPassword = process.env.ADMIN_PASSWORD || 'admin-pebri9290';

  if (username !== expectedUsername) return false;
  if (configuredHash) return bcrypt.compare(password, configuredHash);
  return password === fallbackPassword;
}

