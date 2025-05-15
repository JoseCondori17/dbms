import type { Metadata } from "next";
import { Fira_Code } from 'next/font/google';
import "./globals.css";

export const firaCode = Fira_Code({
  subsets: ['latin'],
  weight: ['400'],
  display: 'swap',
  variable: '--font-fira-code',
});

export const metadata: Metadata = {
  title: "Sonora",
  description: "Created at None",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${firaCode.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
